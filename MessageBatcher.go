package forestbus

import (
	"errors"
	"log"
	"sync"
	"time"
)

// ErrTopicNotFound is returned when the given topic could not be found on the node.
var ErrMessageBatcherBufferFull = errors.New("The message batcher buffer is full.")

// ErrMessageBatcherClosed is returned when the message batcher has been closed and the message not sent.
var ErrMessageBatcherClosed = errors.New("The message batcher was closed before the message could be sent.")

// UNLIMITED_RETRIES can be passed to MessageBatcherRetries to enable unlimited retries for transient errors
const UNLIMITED_RETRIES = -1

const retry_backoff_interval = 500 * time.Millisecond
const retry_backoff_limit = 5 * time.Second
const max_close_wait_time = 2 * time.Second

/*
A MessageBatcher is created using NewMessageBatcher and provides a simple way to batch messages for sending effeciently using an existing Client.
*/
type MessageBatcher struct {
	client              *Client
	topic               string
	waitForCommit       bool
	maxBuffer           int
	pendingChannel      chan *SendResult
	closeChannel        chan chan interface{}
	closeDoneLock       *sync.Mutex
	closeDone           bool
	syncSendResultPool  *sync.Pool
	asyncSendResultPool *sync.Pool
	debug               bool
	maxRetries          int
}

/*
SendResult encapsulates the results from an AsyncSendMessage call.
*/
type SendResult struct {
	index      int64
	err        error
	msg        []byte
	reference  interface{}
	resultChan chan *SendResult
}

/*
GetIndex returns the index of the sent message.  It will have a value of zero if an error occured during the send.
*/
func (res *SendResult) GetIndex() int64 {
	return res.index
}

/*
GetError returns the error encountered sending the message, or nil if it was succesfully sent.
*/
func (res *SendResult) GetError() error {
	return res.err
}

/*
GetReference returns the reference pointer provided in the AsyncSendMessage call.
*/
func (res *SendResult) GetReference() interface{} {
	return res.reference
}

/*
GetMessage returns the original message passed to the AsyncSendMessage call.
*/
func (res *SendResult) GetMessage() []byte {
	return res.msg
}

/*
clean is an internal method used to prepare a SendResult for re-use through the Pool.
*/
func (res *SendResult) clean() {
	res.index = 0
	res.err = nil
	res.msg = nil
	res.reference = nil
}

/*
MessageBatcherConfiguration functions are able to configure options on a MessageBatcher.

The MessageBatcherConfiguration type is exposed externally to facilitate building a list of options programmatically, e.g.:

	options := make ([]forestbus.MessageBatcherConfiguration,0)
	options = append (options, forestbus.MessageBatcherDoNotWaitForCommit())
	options = append (options, forestbus.MessageBatcherSetBufferSize(1000))
	batcher := forestbus.NewMessageBatcher (client, "test-topic", options...)
*/
type MessageBatcherConfiguration func(*MessageBatcher)

/*
MessageBatcherDoNotWaitForCommit configures the MessageBatcher to not wait on commit when sending messages to the cluster.
*/
func MessageBatcherDoNotWaitForCommit() MessageBatcherConfiguration {
	return func(batcher *MessageBatcher) {
		batcher.waitForCommit = false
	}
}

/*
MessageBatcherSetBufferSize allows the default buffer size of 200 to be changed.
*/
func MessageBatcherSetBufferSize(size int) MessageBatcherConfiguration {
	return func(batcher *MessageBatcher) {
		batcher.maxBuffer = size
	}
}

/*
MessageBatcherEnableDebug returns a MessageBatcherConfiguration that can be passed to NewMessageBatcher to enable debug loggging in the library.
*/
func MessageBatcherEnableDebug() MessageBatcherConfiguration {
	return func(batcher *MessageBatcher) {
		batcher.debug = true
	}
}

/*
MessageBatcherEnableRetries returns a MessageBatcherConfiguration that can be passed to NewMessageBatcher to enable automatic retries for transient (e.g. ErrNoNodesAvailable) errors.

Retries start at 500ms intervals, backing off at 500ms intervals until reaching 5s.  If the MessageBatcher is closed while re-trying the error ErrNoNodesAvailable will be passed back to clients calling SendMessage and on the replyChannel of AsyncSendMessage.

The default withtout this configuration option is 0 (no retries), pass forestbus.UNLIMITED_RETRIES to retry without limit.
*/
func MessageBatcherRetries(maxRetries int) MessageBatcherConfiguration {
	return func(batcher *MessageBatcher) {
		batcher.maxRetries = maxRetries
	}
}

/*
NewMessageBatcher returns a new MessageBatcher using the underlying client and for the topic specified.

Optional configuration parameters can be passed to set the commit policy and buffer size.
*/
func NewMessageBatcher(client *Client, topic string, config ...MessageBatcherConfiguration) *MessageBatcher {
	batcher := &MessageBatcher{}
	batcher.client = client
	batcher.waitForCommit = true
	batcher.maxBuffer = 200
	batcher.topic = topic
	batcher.closeDoneLock = &sync.Mutex{}
	batcher.syncSendResultPool = &sync.Pool{New: func() interface{} {
		return &SendResult{resultChan: make(chan *SendResult, 1)}
	}}
	batcher.asyncSendResultPool = &sync.Pool{New: func() interface{} {
		return &SendResult{}
	}}
	for _, cfg := range config {
		cfg(batcher)
	}
	batcher.pendingChannel = make(chan *SendResult, batcher.maxBuffer)
	// closeChannel must be buffered to allow Close() to work even when blocked in Client.
	batcher.closeChannel = make(chan chan interface{}, 1)
	go batcher.sendLoop()
	return batcher
}

/*
SendMessage creates batches of messages and submits them to the Forest Bus cluster using the configuration given in NewMessageBatcher.

If there are no messages pending, the message is sent straight away.  If a client.SendMessages call is in progress then the message will be batched with any others waiting to be sent and sent immediately that the previous call completes.  If blockIfFull is true and the MessageBatcher buffer is full, SendMessage will block until there is space to queue the message.  If blockIfFull is false and the buffer is full, index will be returned as zero and the error ErrMessageBatcherBufferFull will be returned.

SendMessage is synchronous and best used when multiple goroutines are generating messages that need to be sent to the same topic.

SendMessage and AsyncSendMessage can be safely used simultaneously on the same MessageBatcher.
*/
func (batcher *MessageBatcher) SendMessage(message []byte, blockIfFull bool) (index int64, err error) {
	// Catch channel send errors during Close()
	defer func() {
		if r := recover(); r != nil {
			if batcher.debug {
				log.Printf("SendMessage recovering from send while closed.\n")
			}
			index = 0
			err = ErrMessageBatcherClosed
		}
	}()
	// Get a SendResult object for our use
	result := batcher.syncSendResultPool.Get().(*SendResult)
	result.msg = message

	// This will block if we are at maximum capacity
	if blockIfFull {
		if batcher.debug {
			log.Printf("SendMessage queueing message onto channel with %v messages\n", len(batcher.pendingChannel))
		}
		batcher.pendingChannel <- result
		if batcher.debug {
			log.Printf("SendMessage message queued.\n")
		}
	} else {
		select {
		case batcher.pendingChannel <- result:
			if batcher.debug {
				log.Printf("SendMessage message queued.\n")
			}
		default:
			if batcher.debug {
				log.Printf("SendMessage queue not carried out - queue is full and blockIfFull is false.\n")
			}
			return 0, ErrMessageBatcherBufferFull
		}
	}
	// Now block until we have a populated result.
	<-result.resultChan
	index, err = result.index, result.err
	// Clean up the result object and put it back in the pool
	result.clean()
	batcher.syncSendResultPool.Put(result)
	return index, err
}

/*
AsyncSendMessage queues the given message and sends it, along with any other queued messages, using the configuration given in NewMessageBatcher.

The caller MUST ensure that there is sufficient capacity on the replyChannel to avoid contention in the MessageBatcher.  At a minimum set the size of replyChannel to 200 or MessageBatcherSetBufferSize, whichever is greater.

If the caller does not need to know the result of sending the message ("fire and forget"), then pass a nil replyChannel to the call.

If blockIfFull is true and the MessageBatcher buffer is full, AsyncSendMessage will block until there is space to queue the message.  If blockIfFull is false and the buffer is full, the error ErrMessageBatcherBufferFull will be returned and no message will be sent on the replyChannel.

The given reference will be available in the SendResult.GetReference() call, which makes it easier to tie the result of the asynchronous call to objects that may be held.  If this isn't required, pass nil for this value.

SendMessage and AsyncSendMessage can be safely used simultaneously on the same MessageBatcher.
*/
func (batcher *MessageBatcher) AsyncSendMessage(message []byte, reference interface{}, replyChannel chan *SendResult, blockIfFull bool) (err error) {
	// Catch channel send errors during Close()
	defer func() {
		if r := recover(); r != nil {
			if batcher.debug {
				log.Printf("AsyncSendMessage recovering from send while closed.\n")
			}
			err = ErrMessageBatcherClosed
		}
	}()

	// Get a SendResult from the pool
	result := batcher.asyncSendResultPool.Get().(*SendResult)
	result.msg = message
	result.resultChan = replyChannel
	result.reference = reference

	if blockIfFull {
		if batcher.debug {
			log.Printf("AsyncSendMessage queueing message onto channel with %v messages\n", len(batcher.pendingChannel))
		}
		batcher.pendingChannel <- result
		if batcher.debug {
			log.Printf("AsyncSendMessage message queued\n")
		}
	} else {
		select {
		case batcher.pendingChannel <- result:
			if batcher.debug {
				log.Printf("AsyncSendMessage message queued\n")
			}
		default:
			if batcher.debug {
				log.Printf("AsyncSendMessage message not queued as channel is full\n")
			}
			return ErrMessageBatcherBufferFull
		}
	}
	// Now return and let the caller block as they desire
	return nil
}

/*
Close the MessageBatcher.  Pending messages will still be sent if this can complete within 2s, otherwise they will error with ErrMessageBatcherClosed.

If follower nodes are significantly behind, or unreachable from the leader node, then some messages may still be waiting in-flight when Close returns.
These will return with suitable errors when the underlying Client.Close() call is made.

Once the MessageBatcher is closed it cannot be reused.  MessageBatcher.Close is multi-goroutine safe and can be called multiple times.
*/
func (batcher *MessageBatcher) Close() {
	batcher.closeDoneLock.Lock()
	defer batcher.closeDoneLock.Unlock()
	if !batcher.closeDone {
		batcher.closeDone = true
		// Step one - close the pending channel
		close(batcher.pendingChannel)
		// Now let the sending routine know that we are shutting down - this is required to wake up any sleeping we are doing mid-retry
		closeDone := make(chan interface{})
		batcher.closeChannel <- closeDone
		// Now wait for any remaining messages to drain
		timeout := time.NewTimer(max_close_wait_time)
		select {
		case <-closeDone:
		case <-timeout.C:
			// If we've timed out - force drain the queue and error the messages
			for msg := range batcher.pendingChannel {
				msg.err = ErrMessageBatcherClosed
				if msg.resultChan != nil {
					msg.resultChan <- msg
				}
			}
		}
	}
}

/*
sendLoop handles the aggregation of the messages and informing clients of the results.
*/
func (batcher *MessageBatcher) sendLoop() {
	messagesToBeAggregated := make([]*SendResult, 0, batcher.maxBuffer)
	var messagesToSend [][]byte
	messagesToSend = make([][]byte, 0, batcher.maxBuffer)
	totalMessages := 0
	running := true
	var request *SendResult

	var sleepTimer = time.NewTimer(retry_backoff_interval)
	var closeDoneChannel chan interface{}
	for running {
		// Blocking wait for the first message
		var ok bool
		select {
		case closeDoneChannel = <-batcher.closeChannel:
		case request, ok = <-batcher.pendingChannel:
			if ok {
				// We have a new request
				if batcher.debug {
					log.Printf("Batcher receieved message - starting accumulation\n", request, ok)
				}
				messagesToBeAggregated = append(messagesToBeAggregated, request)
				totalMessages++
				gathering := true
				// Now pull as many requests as possible.  When there are none left or we have reached our limit, send them.
				for gathering {
					select {
					case request, ok = <-batcher.pendingChannel:
						if ok {
							// We have additional requests, queue them.
							messagesToBeAggregated = append(messagesToBeAggregated, request)
							totalMessages++
							if totalMessages >= batcher.maxBuffer {
								gathering = false
							}
						} else {
							gathering = false
						}
					default:
						gathering = false
					}
				}
				if batcher.debug {
					log.Printf("Batcher accumulation completed with %v messages\n", len(messagesToBeAggregated))
				}

				// Build the messages to send.
				for _, request := range messagesToBeAggregated {
					messagesToSend = append(messagesToSend, request.msg)
				}

				retry := true
				retryAttempts := 0
				var err error
				var ids []int64

				for retry {
					ids, err = batcher.client.SendMessages(batcher.topic, messagesToSend, batcher.waitForCommit)
					// Cancel the retry if we don't have a transitory error
					if err == nil {
						retry = false
					} else if err != ErrNoNodesAvailable {
						retry = false
					}
					// If we have a retry, go to sleep for a while
					if retry {
						retryAttempts++
						// Check whether we have retried sufficiently.
						if batcher.maxRetries != UNLIMITED_RETRIES && retryAttempts > batcher.maxRetries {
							retry = false
						} else {
							duration := time.Duration(retryAttempts) * retry_backoff_interval
							if duration > retry_backoff_limit {
								duration = retry_backoff_limit
							}
							sleepTimer.Reset(duration)
							// Try sleeping
							select {
							case closeDoneChannel = <-batcher.closeChannel:
								if batcher.debug {
									log.Printf("MessageBatcher closed while sleeping for retry - aborting.\n")
								}
								// Set maxRetries to zero to avoid any further retry attempts during our shutdown
								batcher.maxRetries = 0
								err = ErrMessageBatcherClosed
								retry = false
							case <-sleepTimer.C:
								if batcher.debug {
									log.Printf("MessageBatcher retrying after %v sleeping - attempt %v.\n", duration, retryAttempts)
								}
							}
						}
					}
				}

				// Now update all senders with the results.
				for i, request := range messagesToBeAggregated {
					// Only send the results to those that have provided a channel
					if request.resultChan != nil {
						if err != nil {
							request.index = 0
							request.err = err
						} else {
							request.index = ids[i]
							request.err = nil
						}
						request.resultChan <- request
					} else {
						// If we have an empty channel, keep the object in the pool for future async use
						request.clean()
						batcher.asyncSendResultPool.Put(request)
					}
				}

				messagesToBeAggregated = messagesToBeAggregated[:0]
				messagesToSend = messagesToSend[:0]
				totalMessages = 0
			} else {
				if closeDoneChannel != nil {
					// Queue is empty and closed, time to quit
					running = false
				}
			}
		}
	}

	closeDoneChannel <- struct{}{}
}
