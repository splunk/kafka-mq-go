package redelivery

import (
	"container/heap"
	"time"
)

// This module contains two priority queues implemented using the `container\heap` package.
//
// 1. MarkerOffsetsPQ - used to track markers by their offset in the marker topic in ascending order
// 2. MarkerDeadlinesPQ - used to track markers by their redelivery deadlines in ascending order
//
// Both of the above implementations keep track of their entries and avoid duplicates.
//
// Note that neither of the above are thread-safe.

// MarkerOffset associates a message with the location of the marker that tracks it in the maker topic
type MarkerOffset struct {
	MessageID      MessageID
	OffsetOfMarker int64
}

// MarkerOffsetsPQ is a priority queue to track markers by their offset in the marker topic in ascending order
type MarkerOffsetsPQ struct {
	entries map[MessageID]bool
	heap    markerOffsetsHeap
}

// NewMarkerOffsetsPQ constructs a new `MarkerOffsetsPQ` instance.
func NewMarkerOffsetsPQ() *MarkerOffsetsPQ {
	return &MarkerOffsetsPQ{
		entries: make(map[MessageID]bool),
		heap:    make(markerOffsetsHeap, 0),
	}
}

// Enqueue adds a new `MarkerOffset` item to the priotity queue
func (pq *MarkerOffsetsPQ) Enqueue(markerOffset MarkerOffset) bool {
	if _, ok := pq.entries[markerOffset.MessageID]; ok {
		return false
	}

	pq.entries[markerOffset.MessageID] = true
	heap.Push(&pq.heap, &markerOffset)
	return true
}

// Dequeue removes the `MarkerOffset` item with the smallest offset value from the head of the priotity queue
func (pq *MarkerOffsetsPQ) Dequeue() (MarkerOffset, bool) {
	if pq.isEmpty() {
		return MarkerOffset{}, false
	}

	item := heap.Pop(&pq.heap).(*MarkerOffset)
	delete(pq.entries, item.MessageID)
	return *item, true
}

// Head peeks at the `MarkerOffset` item with the smallest offset value at the head of the queue
// Returns `(MarkerOffset{}, false)` if the queue is empty.
func (pq *MarkerOffsetsPQ) Head() (MarkerOffset, bool) {
	if pq.isEmpty() {
		return MarkerOffset{}, false
	}

	head := pq.heap[0]
	return *head, true
}

func (pq *MarkerOffsetsPQ) isEmpty() bool {
	return pq.heap.Len() == 0
}

// markerOffsetsHeap is a heap used to track markers by their offset in the marker topic in ascending order
type markerOffsetsHeap []*MarkerOffset

func (h markerOffsetsHeap) Len() int {
	return len(h)
}

func (h markerOffsetsHeap) Less(i, j int) bool {
	// We want Pop to give us the smallest, not largest, marker offset so we use less than here.
	return h[i].OffsetOfMarker < h[j].OffsetOfMarker
}

func (h markerOffsetsHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

// Push pushes a `MarkerOffset` onto the heap.
func (h *markerOffsetsHeap) Push(x interface{}) {
	item := x.(*MarkerOffset)
	*h = append(*h, item)
}

// Pop pops a `MarkerOffset` off of the heap.
func (h *markerOffsetsHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// MarkerDeadline associates a message with its redelivery deadline
type MarkerDeadline struct {
	MessageID          MessageID
	RedeliveryDeadline time.Time
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.
}

// MarkerDeadlinesPQ is a priority queue to track messages by their redelivery deadline in ascending order
type MarkerDeadlinesPQ struct {
	entries map[MessageID]*MarkerDeadline
	heap    markerDeadlinesHeap
}

// NewMarkerDeadlinesPQ constructs a new `MarkerDeadlinesPQ` instance.
func NewMarkerDeadlinesPQ() *MarkerDeadlinesPQ {
	return &MarkerDeadlinesPQ{
		entries: make(map[MessageID]*MarkerDeadline),
		heap:    make(markerDeadlinesHeap, 0),
	}
}

// Enqueue adds a new `MarkerDeadline` item to the priotity queue
func (pq *MarkerDeadlinesPQ) Enqueue(markerDeadline MarkerDeadline) bool {
	if _, ok := pq.entries[markerDeadline.MessageID]; ok {
		return false
	}

	item := &markerDeadline
	pq.entries[markerDeadline.MessageID] = item
	heap.Push(&pq.heap, item)
	return true
}

// Dequeue removes the `MarkerDeadline` item with the earliest redelivery deadline from the head of the priotity queue
func (pq *MarkerDeadlinesPQ) Dequeue() (MarkerDeadline, bool) {
	if pq.isEmpty() {
		return MarkerDeadline{}, false
	}

	item := heap.Pop(&pq.heap).(*MarkerDeadline)
	delete(pq.entries, item.MessageID)
	return *item, true
}

// Head peeks at the `MarkerDeadline` item with the earliest redelivery deadline at the head of the queue
// Returns `(MarkerDeadline{}, false)` if the queue is empty.
func (pq *MarkerDeadlinesPQ) Head() (MarkerDeadline, bool) {
	if pq.isEmpty() {
		return MarkerDeadline{}, false
	}

	head := pq.heap[0]
	return *head, true
}

// Update modifies the redelivery deadline of a marker in the priority queue.
func (pq *MarkerDeadlinesPQ) Update(messageID MessageID, redeliveryDeadline time.Time) bool {
	item, ok := pq.entries[messageID]
	if !ok {
		return false
	}

	pq.heap.update(item, redeliveryDeadline)
	return true
}

func (pq *MarkerDeadlinesPQ) isEmpty() bool {
	return pq.heap.Len() == 0
}

// markerDeadlinesHeap is a heap used to track markers by their redelivery deadline in ascending order
type markerDeadlinesHeap []*MarkerDeadline

func (h markerDeadlinesHeap) Len() int {
	return len(h)
}

func (h markerDeadlinesHeap) Less(i, j int) bool {
	// We want Pop to give us the earliest, not latest, redelivery deadline
	return h[i].RedeliveryDeadline.Before(h[j].RedeliveryDeadline)
}

func (h markerDeadlinesHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

// Push pushes a `MarkerDeadline` onto the heap.
func (h *markerDeadlinesHeap) Push(x interface{}) {
	n := len(*h)
	item := x.(*MarkerDeadline)
	item.index = n
	*h = append(*h, item)
}

// Pop pops a `MarkerDeadline` off of the heap.
func (h *markerDeadlinesHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*h = old[0 : n-1]
	return item
}

// Update modifies the redelivery deadline of a marker in the heap.
func (h *markerDeadlinesHeap) update(item *MarkerDeadline, redeliveryDeadline time.Time) {
	item.RedeliveryDeadline = redeliveryDeadline
	heap.Fix(h, item.index)
}
