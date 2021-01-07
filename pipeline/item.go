package pipeline

import (
	"time"
)

type Item struct {
	Id    int
	Start time.Time
}

func NewItem(id int) Item {
	return Item{
		Id:    id,
		Start: time.Now(),
	}
}

func (item *Item) Duration() time.Duration {
	return time.Since(item.Start)
}
