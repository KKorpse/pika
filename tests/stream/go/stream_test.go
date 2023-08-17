package pika_integration

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
)

func streamCompareID(a, b string) int {
	if a == b {
		return 0
	}
	aParts := strings.Split(a, "-")
	bParts := strings.Split(b, "-")
	aMs, _ := strconv.ParseInt(aParts[0], 10, 64)
	aSeq, _ := strconv.ParseInt(aParts[1], 10, 64)
	bMs, _ := strconv.ParseInt(bParts[0], 10, 64)
	bSeq, _ := strconv.ParseInt(bParts[1], 10, 64)
	if aMs > bMs {
		return 1
	}
	if aMs < bMs {
		return -1
	}
	if aSeq > bSeq {
		return 1
	}
	if aSeq < bSeq {
		return -1
	}
	return 0
}

func streamNextID(id string) string {
	parts := strings.Split(id, "-")
	ms := parts[0]
	seq, _ := strconv.ParseInt(parts[1], 10, 64)
	seq++
	return fmt.Sprintf("%s-%d", ms, seq)
}

func streamRandomID(minID, maxID string) string {
	minParts := strings.Split(minID, "-")
	maxParts := strings.Split(maxID, "-")
	minMs, _ := strconv.Atoi(minParts[0])
	maxMs, _ := strconv.Atoi(maxParts[0])
	delta := maxMs - minMs + 1
	ms := minMs + randomInt(delta)
	seq := randomInt(1000)
	return fmt.Sprintf("%d-%d", ms, seq)
}

func streamSimulateXRANGE(items []string, start, end string) []string {
	var res []string
	for _, item := range items {
		thisID := strings.Split(item, " ")[0]
		if streamCompareID(thisID, start) >= 0 {
			if streamCompareID(thisID, end) <= 0 {
				res = append(res, item)
			}
		}
	}
	return res
}

// Helper function for generating random integers
func randomInt(n int) int {
	return rand.Intn(n)
}

var _ = Describe("Stream Commands", func() {
	ctx := context.TODO()
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(pikaOptions1())
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	Describe("streams", func() {
		PIt("XADD wrong number of args", func() {
			_, err := client.Do(ctx, "XADD", "mystream").Result()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("wrong number of arguments"))

			_, err = client.Do(ctx, "XADD", "mystream", "*").Result()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("wrong number of arguments"))

			_, err = client.Do(ctx, "XADD", "mystream", "*", "field").Result()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("wrong number of arguments"))
		})

		PIt("XADD can add entries into a stream that XRANGE can fetch", func() {
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: []string{"item", "1", "value", "a"}}).Err()).NotTo(HaveOccurred())
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: []string{"item", "2", "value", "b"}}).Err()).NotTo(HaveOccurred())

			Expect(client.XLen(ctx, "mystream").Val()).To(Equal(int64(2)))

			items := client.XRange(ctx, "mystream", "-", "+").Val()
			Expect(len(items)).To(Equal(2))
			Expect(items[0].Values).To(Equal(map[string]interface{}{"item": "1", "value": "a"}))
			Expect(items[1].Values).To(Equal(map[string]interface{}{"item": "2", "value": "b"}))
		})

		PIt("XADD IDs are incremental", func() {
			id1 := client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: []string{"item", "1", "value", "a"}}).Val()
			id2 := client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: []string{"item", "2", "value", "b"}}).Val()
			id3 := client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: []string{"item", "3", "value", "c"}}).Val()

			Expect(streamCompareID(id1, id2)).To(Equal(-1))
			Expect(streamCompareID(id2, id3)).To(Equal(-1))
		})

		// FIXME: pika may not support MULTI ?
		// It("XADD IDs are incremental when ms is the same as well", func() {
		// 	client.Do(ctx, "MULTI")
		// 	Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: []string{"item", "1", "value", "a"}}).Err()).NotTo(HaveOccurred())
		// 	Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: []string{"item", "2", "value", "b"}}).Err()).NotTo(HaveOccurred())
		// 	Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: []string{"item", "3", "value", "c"}}).Err()).NotTo(HaveOccurred())
		// 	res := client.Do(ctx, "EXEC").Val().([]interface{})
		// 	Expect(len(res)).To(Equal(3))
		// 	Expect(streamCompareID(res[0].(string), res[1].(string))).To(Equal(-1))
		// 	Expect(streamCompareID(res[1].(string), res[2].(string))).To(Equal(-1))
		// })

		PIt("XADD IDs correctly report an error when overflowing", func() {
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "18446744073709551615-18446744073709551615", Values: []string{"a", "b"}}).Err()).NotTo(HaveOccurred())
			_, err := client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "*", Values: []string{"c", "d"}}).Result()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("ERR"))
		})

		PIt("XADD auto-generated sequence is incremented for last ID", func() {
			x1 := client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "123-456", Values: []string{"item", "1", "value", "a"}}).Val()
			x2 := client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "123-*", Values: []string{"item", "2", "value", "b"}}).Val()
			Expect(x2).To(Equal("123-457"))
			Expect(streamCompareID(x1, x2)).To(Equal(-1))
		})

		PIt("XADD auto-generated sequence is zero for future timestamp ID", func() {
			x1 := client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "123-456", Values: []string{"item", "1", "value", "a"}}).Val()
			x2 := client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "789-*", Values: []string{"item", "2", "value", "b"}}).Val()
			Expect(x2).To(Equal("789-0"))
			Expect(streamCompareID(x1, x2)).To(Equal(-1))
		})

		PIt("XADD auto-generated sequence can't be smaller than last ID", func() {
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "123-456", Values: []string{"item", "1", "value", "a"}}).Err()).NotTo(HaveOccurred())
			_, err := client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "42-*", Values: []string{"item", "2", "value", "b"}}).Result()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("ERR"))
		})

		PIt("XADD auto-generated sequence can't overflow", func() {
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "1-18446744073709551615", Values: []string{"a", "b"}}).Err()).NotTo(HaveOccurred())
			_, err := client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "1-*", Values: []string{"c", "d"}}).Result()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("ERR"))
		})

		PIt("XADD 0-* should succeed", func() {
			x := client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "0-*", Values: []string{"a", "b"}}).Val()
			Expect(x).To(Equal("0-1"))
		})

		It("XADD with MAXLEN option", func() {
			Expect(client.Del(ctx, "mystream").Err()).NotTo(HaveOccurred())
			for i := 0; i < 1000; i++ {
				if rand.Float64() < 0.9 {
					Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", MaxLen: 5, Values: []string{"xitem", strconv.Itoa(i)}}).Err()).NotTo(HaveOccurred())
				} else {
					Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", MaxLen: 5, Values: []string{"yitem", strconv.Itoa(i)}}).Err()).NotTo(HaveOccurred())
				}
			}
			Expect(client.XLen(ctx, "mystream").Val()).To(Equal(int64(5)))
			items := client.XRange(ctx, "mystream", "-", "+").Val()
			expected := 995
			for _, item := range items {
				Expect(item.Values).To(SatisfyAny(
					HaveKeyWithValue("xitem", strconv.Itoa(expected)),
					HaveKeyWithValue("yitem", strconv.Itoa(expected)),
				))
				expected++
			}
		})

	})
})
