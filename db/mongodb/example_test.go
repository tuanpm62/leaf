package mongodb_test

import (
	"context"
	"fmt"
	"leaf/db/mongodb"
)

func Example() {
	c, err := mongodb.Connect("mongodb://localhost:27017", 10)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer c.Close()

	// client
	s, err := c.Ref()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer c.UnRef(s)

	// Remove test document if it exists
	collRef := s.Database("test").Collection("counters")
	_, err = collRef.DeleteOne(context.TODO(), map[string]interface{}{"_id": "test"})
	// No need to check for NotFound error as DeleteOne doesn't return it

	// auto increment
	err = c.EnsureCounter("test", "counters", "test")
	if err != nil {
		fmt.Println(err)
		return
	}
	for i := 0; i < 3; i++ {
		id, err := c.NextSeq("test", "counters", "test")
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println(id)
	}

	// index
	c.EnsureUniqueIndex("test", "counters", []string{"key1"})

	// Output:
	// 1
	// 2
	// 3
}
