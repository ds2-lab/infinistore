package server

import (
	"time"

	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/wangaoone/LambdaObjectstore/proxy/global"
)

// reuse window and interval should be MINUTES
type MovingWindow struct {
	log      logger.ILogger
	window   int
	interval int
	num      int
	buckets  []*bucket

	cursor int

	startTime time.Time
}

func NewMovingWindow(window int, interval int) *MovingWindow {
	// number of active time bucket
	num := window / interval
	return &MovingWindow{
		log: &logger.ColorLogger{
			Prefix: "Moving window ",
			Level:  global.Log.GetLevel(),
			Color:  true,
		},
		window:    window,
		interval:  interval,
		num:       num,
		buckets:   make([]*bucket, 0, num*2),
		startTime: time.Now(),
		cursor:    0,
	}
}

func (mw *MovingWindow) start(ready chan struct{}) *Group {
	bucket := NewBucket(0, ready)
	mw.buckets = append(mw.buckets, bucket)
	return bucket.group
}

func (mw *MovingWindow) Rolling() {
	idx := 1
	for {
		ticker := time.NewTicker(time.Duration(mw.interval) * time.Minute)
		select {
		case <-ticker.C:
			mw.buckets = append(mw.buckets, NewBucket(idx, make(chan struct{})))
			if len(mw.buckets) > mw.num {
				mw.buckets = mw.buckets[1:]
			}
			mw.cursor = len(mw.buckets) - 1
			//fmt.Println(mw.buckets, mw.cursor)
		}
		idx += 1
	}
}

// retrieve cold bucket (first half)
func (mw *MovingWindow) getCold() []*bucket {
	if len(mw.buckets) < mw.num {
		return nil
	} else {
		return mw.buckets[0 : mw.num/2-1]
	}
}

// retrieve hot bucket (second half)
func (mw *MovingWindow) getActive() []*bucket {
	if len(mw.buckets) < mw.num {
		return mw.buckets
	} else {
		return mw.buckets[mw.num/2:]
	}
}

func (mw *MovingWindow) touch(meta *Meta) {
	// brand new meta(-1) or already existed
	if meta.placerMeta.bucketIdx == -1 {
		mw.buckets[mw.cursor].m.Set(meta.Key, meta)
	} else {
		// remove meta from previous bucket
		oldBucket := meta.placerMeta.bucketIdx
		if mw.cursor == oldBucket {
			return
		} else {
			mw.buckets[oldBucket].m.Del(meta.Key)
			mw.buckets[mw.cursor].m.Set(meta.Key, meta)
		}
	}

	meta.placerMeta.bucketIdx = mw.cursor
	meta.placerMeta.ts = time.Now().UnixNano()
}

//func (mw *MovingWindow) updateBucket(meta *Meta, lastChunk int) {
//	oldBucket := meta.placerMeta.bucketIdx
//
//	if oldBucket == -1 {
//		mw.log.Debug("Not found in moving window, please set it first")
//		return
//	}
//
//	if mw.cursor == oldBucket {
//		return
//	} else {
//		mw.buckets[oldBucket].m.Del(meta.Key)
//		mw.buckets[mw.cursor].m.Set(meta.Key, meta)
//		meta.placerMeta.bucketIdx = mw.cursor
//	}
//	meta.placerMeta.ts = time.Now().UnixNano()
//
//}

//func (mw *MovingWindow) cursorMove() {
//	new := mw.cursor - 1
//	if new < 0 {
//		mw.cursor = len(mw.buckets) - 1
//	}
//	mw.cursor = new
//}

// last bucket is @ the left hand of the current cursor
//func (mw *MovingWindow) getLastBucket() int {
//	// All buckets are not full, first bucket is the oldest
//	if int(time.Now().Sub(mw.startTime).Minutes()) <= mw.window {
//		return 0
//	}
//	idx := mw.cursor - 1
//	if idx < 0 {
//		mw.cursor = len(mw.buckets) - 1
//	}
//	return idx
//}

//func (mw *MovingWindow) removeOldest(bucketIdx int) (*Meta, bool) {
//	if mw.buckets[bucketIdx].size() == 0 {
//		return nil, false
//	}
//	var res *Meta
//	bucket := mw.buckets[bucketIdx]
//
//	for i := range bucket.m.Iter() {
//		ts := i.Value.(*Meta).placerMeta.ts
//		if ts < oldest {
//			oldest = ts
//			res = i.Value.(*Meta)
//		}
//	}
//	mw.buckets[bucketIdx].m.Del(res.Key)
//	return res, true
//}

func (mw *MovingWindow) evict(idx int) {
	// TODO: all the metas in last bucket need to be evicted
}

//func (mw *MovingWindow) Check() {
//	for {
//		t := time.NewTicker(time.Duration(mw.interval) * time.Minute)
//		select {
//		case <-t.C:
//			mw.evict(mw.getLastBucket())
//			mw.cursorMove()
//		}
//	}
//}

//func (mw *MovingWindow) Start() {
//	mw.start = time.Now()
//}
//
//func (mw *MovingWindow) getBucket(meta *Meta) int {
//	delta := meta.placerMeta.currentTS.Sub(mw.start).Minutes()
//	windowIdx := int(delta)/mw.interval + 1
//	//return int(math.Mod(delta, float64(mw.interval)))
//	return windowIdx
//}
//
//func (mw *MovingWindow) Add(meta *Meta) bool {
//	// get bucket idx
//	meta.movingMeta.bucketIdx = mw.getBucket(meta)
//	_, loaded := mw.buckets[meta.movingMeta.bucketIdx].GetOrInsert(meta.Key, meta)
//
//	if !loaded {
//		mw.log.Debug("Already stored")
//		return loaded
//	}
//
//	return loaded
//}
//
//func (mw *MovingWindow) Update(meta *Meta) {
//	delta := int(meta.placerMeta.reuseTime.Minutes())
//	if delta < mw.reuseDistance {
//
//		newIdx := mw.getBucket(meta)
//		lastIdx := meta.movingMeta.bucketIdx
//
//		// remove previous bucket meta
//		mw.buckets[lastIdx].Del(meta.Key)
//		mw.buckets[newIdx].Set(meta.Key, meta)
//	}
//}

// each bucket check and move the validate meta
//func (mw *MovingWindow) bucketCheck(idx int, wg *sync.WaitGroup) {
//	bucket := mw.buckets[idx]
//	for i := range bucket.Iter() {
//		meta := i.Value.(*Meta)
//
//		// never be touched after first insert
//		if meta.placerMeta.lastTS.IsZero() {
//			meta.placerMeta.reuseTime = time.Now().Sub(meta.placerMeta.currentTS)
//		} else {
//			// update meta reuse distance
//			meta.placerMeta.reuseTime += CheckInterval
//		}
//		if int(meta.placerMeta.reuseTime.Minutes()) > mw.window {
//			// timeout, del key and move to evict list
//			bucket.Del(i.Key)
//			mw.window = append(mw.evictQueue, meta)
//		} else {
//			// TODO: move to other bucket idx
//			newIdx := mw.findBucket(meta)
//
//			// move to other corresponding bucket
//			if newIdx != idx {
//				bucket.Del(i.Key)
//				mw.buckets[newIdx].Set(i.Key, meta)
//			}
//
//		}
//
//	}
//	wg.Done()
//
//}
