package storage

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkSet(b *testing.B) {
	s := NewStore[string, string](StoreOptions{NumShards: 64})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Set(fmt.Sprintf("key-%d", i%1000), "value", 0)
	}
}

func BenchmarkGet(b *testing.B) {
	s := NewStore[string, string](StoreOptions{NumShards: 64})
	for i := 0; i < 1000; i++ {
		s.Set(fmt.Sprintf("key-%d", i), "value", 0)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Get(fmt.Sprintf("key-%d", i%1000))
	}
}

func BenchmarkConcurrentGet(b *testing.B) {
	s := NewStore[string, string](StoreOptions{NumShards: 64})
	for i := 0; i < 1000; i++ {
		s.Set(fmt.Sprintf("key-%d", i), "value", 0)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s.Get(fmt.Sprintf("key-%d", i%1000))
			i++
		}
	})
}

func BenchmarkConcurrentMixed(b *testing.B) {
	s := NewStore[string, string](StoreOptions{NumShards: 64})
	for i := 0; i < 1000; i++ {
		s.Set(fmt.Sprintf("key-%d", i), "value", 0)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%10 == 0 {
				s.Set(fmt.Sprintf("key-%d", i%1000), "new-value", time.Now().Add(time.Minute).UnixNano())
			} else {
				s.Get(fmt.Sprintf("key-%d", i%1000))
			}
			i++
		}
	})
}
