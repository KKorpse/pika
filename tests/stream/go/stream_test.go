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

		It("XADD can add entries into a stream that XRANGE can fetch", func() {
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: []string{"item", "1", "value", "a"}}).Err()).NotTo(HaveOccurred())
			Expect(client.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: []string{"item", "2", "value", "b"}}).Err()).NotTo(HaveOccurred())

			Expect(client.XLen(ctx, "mystream").Val()).To(Equal(int64(2)))

			items := client.XRange(ctx, "mystream", "-", "+").Val()
			Expect(len(items)).To(Equal(2))
			Expect(items[0].Values).To(Equal(map[string]interface{}{"item": "1", "value": "a"}))
			Expect(items[1].Values).To(Equal(map[string]interface{}{"item": "2", "value": "b"}))
		})

	})
})
