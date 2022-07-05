package directory

import (
	"sync"
)

type S3Worker interface {
	RemoteKey() string
	LocalPath() string
	DoWork()
}

// Download worker
type S3DownloadWorker struct {
	remoteKey string
	localPath string
	util      S3Util
}

func (downloadWorker *S3DownloadWorker) RemoteKey() string {
	return downloadWorker.remoteKey
}
func (downloadWorker *S3DownloadWorker) LocalPath() string {
	return downloadWorker.localPath
}
func (downloadWorker *S3DownloadWorker) DoWork() {
	downloadWorker.util.DownloadFile(downloadWorker.remoteKey, downloadWorker.localPath)
}

func NewDownloadWorker(remoteKey string, localPath string, util S3Util) *S3DownloadWorker {
	return &S3DownloadWorker{
		remoteKey: remoteKey,
		localPath: localPath,
		util:      util,
	}
}

// Upload worker
type S3UploadWorker struct {
	remoteKey string
	localPath string
	util      S3Util
}

func (uploadWorker *S3UploadWorker) RemoteKey() string {
	return uploadWorker.remoteKey
}
func (uploadWorker *S3UploadWorker) LocalPath() string {
	return uploadWorker.localPath
}
func (uploadWorker *S3UploadWorker) DoWork() {
	uploadWorker.util.UploadFile(uploadWorker.remoteKey, uploadWorker.localPath)
}

func NewUploadWorker(remoteKey string, localPath string, util S3Util) *S3UploadWorker {
	return &S3UploadWorker{
		remoteKey: remoteKey,
		localPath: localPath,
		util:      util,
	}
}

type S3WorkerNode struct {
	prev   *S3WorkerNode
	next   *S3WorkerNode
	Worker S3Worker
}

type S3WorkerQueueIterator struct {
	next *S3WorkerNode
}

func (iterator *S3WorkerQueueIterator) hasNext() bool {
	return iterator.next != nil
}
func (iterator *S3WorkerQueueIterator) getNext() *S3WorkerNode {
	oldNext := iterator.next
	iterator.next = oldNext.next
	return oldNext
}

type S3WorkerQueue struct {
	head  *S3WorkerNode
	index map[string]*S3WorkerNode
	mutex sync.Mutex
}

func (queue *S3WorkerQueue) Size() int {
	return len(queue.index)
}
func (queue *S3WorkerQueue) IsEmpty() bool {
	return queue.head == nil && queue.Size() == 0
}
func (queue *S3WorkerQueue) Add(worker S3Worker) bool {
	queue.mutex.Lock()
	defer queue.mutex.Unlock()
	_, exists := queue.index[worker.RemoteKey()]
	if exists {
		return false
	}
	node := &S3WorkerNode{
		Worker: worker,
	}
	if queue.head == nil {
		queue.head = node
	} else {
		oldHead := queue.head
		oldHead.prev = node

		node.next = oldHead
		queue.head = node
	}
	queue.index[worker.RemoteKey()] = node
	return true
}
func (queue *S3WorkerQueue) Remove(worker S3Worker) bool {
	queue.mutex.Lock()
	defer queue.mutex.Unlock()
	node, exists := queue.index[worker.RemoteKey()]
	if !exists {
		return false
	}
	if queue.head == node {
		queue.head = node.next
	} else {
		if node.prev != nil {
			node.prev.next = node.next
			if node.next != nil {
				node.next.prev = node.prev
			}
		}
	}
	delete(queue.index, worker.RemoteKey())
	return true
}

func (queue *S3WorkerQueue) DoWork() {
	wg := &sync.WaitGroup{}
	iterator := S3WorkerQueueIterator{
		next: queue.head,
	}
	for iterator.hasNext() {
		wg.Add(1)
		go func(worker *S3WorkerNode, q *S3WorkerQueue, g *sync.WaitGroup) {
			defer g.Done()
			if worker != nil {
				worker.Worker.DoWork()
				q.Remove(worker.Worker)
			}
		}(iterator.getNext(), queue, wg)
	}
	wg.Wait()
}

func NewWorkerQueue() *S3WorkerQueue {
	return &S3WorkerQueue{
		head:  nil,
		index: map[string]*S3WorkerNode{},
		mutex: sync.Mutex{},
	}
}
