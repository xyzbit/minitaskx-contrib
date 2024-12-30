package mysql

import (
	"context"
	"errors"
	"time"

	"github.com/samber/lo"
	"github.com/xyzbit/minitaskx/core/components/taskrepo"
	"github.com/xyzbit/minitaskx/core/model"
	"gorm.io/gorm"
)

type taskRepoImpl struct {
	db *gorm.DB
}

func NewTaskRepo(db *gorm.DB) taskrepo.Interface {
	return &taskRepoImpl{
		db: db,
	}
}

func (t *taskRepoImpl) CreateTask(ctx context.Context, task *model.Task) error {
	taskPo := FromTaskModel(task)
	taskRunPo := &TaskRun{
		TaskKey:   task.TaskKey,
		WorkerID:  task.WorkerID,
		NextRunAt: task.NextRunAt,
	}

	return t.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(taskPo).Error; err != nil {
			return err
		}
		return tx.Create(taskRunPo).Error
	})
}

func (t *taskRepoImpl) UpdateTask(ctx context.Context, task *model.Task) error {
	if task.TaskKey == "" {
		return errors.New("task key is empty")
	}

	taskPo := FromTaskModel(task)

	return t.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := t.db.WithContext(ctx).Model(&Task{}).
			Where("task_key = ?", task.TaskKey).
			Updates(&taskPo).Error; err != nil {
			return err
		}

		if task.Status.IsFinalStatus() {
			return tx.Model(&TaskRun{}).
				Where("task_key = ?", task.TaskKey).
				Delete(&TaskRun{}).Error
		}

		if taskPo.WorkerID != "" || taskPo.NextRunAt != nil {
			if err := tx.Model(&TaskRun{}).
				Where("task_key = ?", task.TaskKey).
				Updates(&TaskRun{
					TaskKey:   task.TaskKey,
					WorkerID:  task.WorkerID,
					NextRunAt: task.NextRunAt,
				}).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

func (t *taskRepoImpl) GetTask(ctx context.Context, taskKey string) (*model.Task, error) {
	var task Task
	if err := t.db.WithContext(ctx).
		Model(&Task{}).
		Where("task_key = ?", taskKey).
		First(&task).Error; err != nil {
		return nil, err
	}
	return ToTaskModel(&task), nil
}

func (t *taskRepoImpl) BatchGetTask(ctx context.Context, taskKeys []string) ([]*model.Task, error) {
	if len(taskKeys) == 0 {
		return nil, nil
	}
	var tasks []*Task
	if err := t.db.WithContext(ctx).
		Model(&Task{}).
		Where("task_key in (?)", taskKeys).
		Find(&tasks).Error; err != nil {
		return nil, err
	}

	return lo.Map(tasks, func(item *Task, index int) *model.Task {
		return ToTaskModel(item)
	}), nil
}

func (t *taskRepoImpl) ListTask(ctx context.Context, filter *model.TaskFilter) ([]*model.Task, error) {
	var tasks []*Task

	tx := t.db.WithContext(ctx).Model(&Task{})
	if filter != nil {
		if len(filter.BizIDs) > 0 {
			tx = tx.Where("biz_id in (?)", filter.BizIDs)
		}
		if filter.BizType != "" {
			tx = tx.Where("biz_type = ?", filter.BizType)
		}
		if filter.Type != "" {
			tx = tx.Where("type = ?", filter.Type)
		}
	}
	if err := tx.Limit(filter.Limit).Offset(filter.Offset).Find(&tasks).Error; err != nil {
		return nil, err
	}

	return lo.Map(tasks, func(item *Task, index int) *model.Task {
		return ToTaskModel(item)
	}), nil
}

func (t *taskRepoImpl) ListRunnableTasks(ctx context.Context, workerID string) ([]string, error) {
	var taskRuns []*TaskRun
	tx := t.db.WithContext(ctx).Model(&TaskRun{}).
		Where("next_run_at <= ?", time.Now())

	if workerID != "" {
		tx = tx.Where("worker_id = ?", workerID)
	}

	if err := tx.Find(&taskRuns).Error; err != nil {
		return nil, err
	}

	return lo.Map(taskRuns, func(item *TaskRun, _ int) string {
		return item.TaskKey
	}), nil
}

// watch all runnable tasks change.
func (t *taskRepoImpl) WatchRunnableTasks(ctx context.Context, workerID string) (<-chan []string, error) {
	keysCh := make(chan []string)
	// Warn mysql has no native watch feature
	return keysCh, nil
}
