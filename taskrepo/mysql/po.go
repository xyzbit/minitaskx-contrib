package mysql

import (
	"encoding/json"
	"time"

	"github.com/xyzbit/minitaskx/core/model"
)

type Task struct {
	ID            int64      `gorm:"column:id;primaryKey;autoIncrement"`
	TaskKey       string     `gorm:"column:task_key;not null;comment:任务唯一标识"`
	BizID         string     `gorm:"column:biz_id"`
	BizType       string     `gorm:"column:biz_type"`
	Type          string     `gorm:"column:type;not null;comment:任务类型"`
	Payload       string     `gorm:"column:payload;not null;comment:任务内容"`
	Labels        *string    `gorm:"column:labels;type:json;comment:任务标签"`
	Stains        *string    `gorm:"column:staints;type:json;comment:任务污点"`
	Extra         *string    `gorm:"column:extra"`
	Status        string     `gorm:"column:status;not null;comment:pending scheduled running|puase success failed"`
	WantRunStatus string     `gorm:"column:want_run_status;not null;comment:期望状态"`
	WorkerID      string     `gorm:"column:worker_id;not null;comment:工作者id"`
	NextRunAt     *time.Time `gorm:"column:next_run_at;comment:下次执行时间"`
	Msg           string     `gorm:"column:msg"`
	CreatedAt     time.Time  `gorm:"column:created_at;autoCreateTime"`
	UpdatedAt     time.Time  `gorm:"column:updated_at;autoUpdateTime"`
}

func (Task) TableName() string {
	return "task"
}

type TaskRun struct {
	ID        int64      `gorm:"column:id;primaryKey;autoIncrement"`
	TaskKey   string     `gorm:"column:task_key;not null;comment:任务唯一标识"`
	WorkerID  string     `gorm:"column:worker_id;not null;comment:工作者id"`
	NextRunAt *time.Time `gorm:"column:next_run_at;comment:下一次执行时间"`
	CreatedAt time.Time  `gorm:"column:created_at;autoCreateTime"`
	UpdatedAt time.Time  `gorm:"column:updated_at;autoUpdateTime"`
}

func (TaskRun) TableName() string {
	return "task_run"
}

func FromTaskModel(t *model.Task) *Task {
	if t == nil {
		return nil
	}

	task := &Task{
		ID:            t.ID,
		TaskKey:       t.TaskKey,
		BizID:         t.BizID,
		BizType:       t.BizType,
		Type:          t.Type,
		Payload:       t.Payload,
		Status:        t.Status.String(),
		WantRunStatus: t.WantRunStatus.String(),
		WorkerID:      t.WorkerID,
		NextRunAt:     t.NextRunAt,
		Msg:           t.Msg,
		CreatedAt:     t.CreatedAt,
		UpdatedAt:     t.UpdatedAt,
	}
	if t.Labels != nil {
		labels, _ := json.Marshal(t.Labels)
		labelsStr := string(labels)
		task.Labels = &labelsStr
	}
	if t.Stains != nil {
		stains, _ := json.Marshal(t.Stains)
		stainsStr := string(stains)
		task.Stains = &stainsStr
	}
	if t.Extra != nil {
		extra, _ := json.Marshal(t.Extra)
		extraStr := string(extra)
		task.Extra = &extraStr
	}

	return task
}

func ToTaskModel(t *Task) *model.Task {
	if t == nil {
		return nil
	}

	task := &model.Task{
		ID:            t.ID,
		TaskKey:       t.TaskKey,
		BizID:         t.BizID,
		BizType:       t.BizType,
		Type:          t.Type,
		Payload:       t.Payload,
		Status:        model.TaskStatus(t.Status),
		WantRunStatus: model.TaskStatus(t.WantRunStatus),
		WorkerID:      t.WorkerID,
		NextRunAt:     t.NextRunAt,
		Msg:           t.Msg,
		CreatedAt:     t.CreatedAt,
		UpdatedAt:     t.UpdatedAt,
	}

	if t.Labels != nil && *t.Labels != "" {
		var labels map[string]string
		_ = json.Unmarshal([]byte(*t.Labels), &labels)
		task.Labels = labels
	}
	if t.Stains != nil && *t.Labels != "" {
		var stains map[string]string
		_ = json.Unmarshal([]byte(*t.Stains), &stains)
		task.Stains = stains
	}
	if t.Extra != nil && *t.Extra != "" {
		var extra map[string]string
		_ = json.Unmarshal([]byte(*t.Extra), &extra)
		task.Extra = extra
	}

	return task
}
