package utils

import (
	"cmp"
	"slices"
)

// OrderedList is a list that is sorted
type OrderedList[T cmp.Ordered] struct {
	list []T
}

func NewOrderedList[T cmp.Ordered]() *OrderedList[T] {
	return &OrderedList[T]{
		list: make([]T, 0),
	}
}

func (o *OrderedList[T]) Len() int {
	return len(o.list)
}

func (o *OrderedList[T]) ToSet() map[T]struct{} {
	m := make(map[T]struct{}, len(o.list))
	for _, item := range o.list {
		m[item] = struct{}{}
	}
	return m
}

func (o *OrderedList[T]) Add(item T) *OrderedList[T] {
	i, _ := slices.BinarySearch(o.list, item)
	// Make room for new value and add it
	o.list = append(o.list, *new(T))
	copy(o.list[i+1:], o.list[i:])
	o.list[i] = item
	return o
}

func (o *OrderedList[T]) AddNoDuplicate(item T) *OrderedList[T] {
	i, found := slices.BinarySearch(o.list, item)
	if found {
		return o
	}
	// Make room for new value and add it
	o.list = append(o.list, *new(T))
	copy(o.list[i+1:], o.list[i:])
	o.list[i] = item
	return o
}

// Remove removes the item from the list _once_ if it exists
func (o *OrderedList[T]) Remove(item T) *OrderedList[T] {
	i, found := slices.BinarySearch(o.list, item)
	if !found {
		return o
	}
	copy(o.list[i:], o.list[i+1:])
	// release the reference
	o.list[len(o.list)-1] = *new(T)
	o.list = o.list[:len(o.list)-1]
	return o
}

// Clone returns a copy of the list
func (o *OrderedList[T]) Clone() *OrderedList[T] {
	return &OrderedList[T]{
		list: slices.Clone(o.list),
	}
}

// GetUnderlyingList returns the underlying list
// take care of the returned list
func (o *OrderedList[T]) GetUnderlyingList() []T {
	return o.list
}

// UnorderedList is a list that is not sorted
type UnorderedList[T cmp.Ordered] struct {
	list []T
}

func NewUnorderedList[T cmp.Ordered]() *UnorderedList[T] {
	return &UnorderedList[T]{
		list: make([]T, 0),
	}
}

func (o *UnorderedList[T]) Len() int {
	return len(o.list)
}

func (o *UnorderedList[T]) ToSet() map[T]struct{} {
	m := make(map[T]struct{}, len(o.list))
	for _, item := range o.list {
		m[item] = struct{}{}
	}
	return m
}

func (o *UnorderedList[T]) Add(item T) *UnorderedList[T] {
	o.list = append(o.list, item)
	return o
}

func (o *UnorderedList[T]) AddNoDuplicate(item T) *UnorderedList[T] {
	i := slices.Index(o.list, item)
	if i != -1 {
		return o
	}
	// Make room for new value and add it
	o.list = append(o.list, item)
	return o
}

// Remove removes the item from the list _once_ if it exists
func (o *UnorderedList[T]) Remove(item T) *UnorderedList[T] {
	i := slices.Index(o.list, item)
	if i == -1 {
		return o
	}
	copy(o.list[i:], o.list[i+1:])
	// release the reference
	o.list[len(o.list)-1] = *new(T)
	o.list = o.list[:len(o.list)-1]
	return o
}

// Clone returns a copy of the list
func (o *UnorderedList[T]) Clone() *UnorderedList[T] {
	return &UnorderedList[T]{
		list: slices.Clone(o.list),
	}
}

// GetUnderlyingList returns the underlying list
// take care of the returned list
func (o *UnorderedList[T]) GetUnderlyingList() []T {
	return o.list
}
