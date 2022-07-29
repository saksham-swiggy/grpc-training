package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	calculator_pb "github.com/saksham-swiggy/grpc-training/calculator_pb/gen"
	"google.golang.org/grpc"
)

func main() {

	fmt.Println("Beginning sending requests")

	cc, err := grpc.Dial("localhost:8000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("could not connect: %v", err)
	}
	defer cc.Close()

	c := calculator_pb.NewCalculatorServiceClient(cc)

	//Unary API function - Calculator
	Sum(c)

	// Server-Side Streaming function - PrimeNumber
	PrimeNumber(c)

	//Client-Side Streaming function - ComputeAverage
	ComputeAverage(c)

	//bidirectional streaming function - FindMaxNumber
	FindMaxNumber(c)

	fmt.Println("All API calls completed")
}

func Sum(c calculator_pb.CalculatorServiceClient) {

	fmt.Println("Starting to do a unary gRPC")

	req := calculator_pb.SumRequest{
		Num1: 5.5,
		Num2: 7.4,
	}

	resp, err := c.Sum(context.Background(), &req)
	if err != nil {
		log.Fatalf("error while calling sum gRPC unary call: %v", err)
	}

	log.Printf("Response from Unary Call, Sum : %v", resp.GetSum())

}

func PrimeNumber(c calculator_pb.CalculatorServiceClient) {
	fmt.Println("Staring ServerSide gRPC streaming")

	req := calculator_pb.PrimeNumberRequest{
		Limit: 15,
	}

	respStream, err := c.PrimeNumber(context.Background(), &req)
	if err != nil {
		log.Fatalf("error while calling Prime Numbers server-side streaming gRPC : %v", err)
	}

	for {
		msg, err := respStream.Recv()
		if err == io.EOF {
			break
		}

		if err != nil {
			log.Fatalf("error while receving server stream : %v", err)
		}

		log.Println("Response From Server, Prime Number : ", msg.GetPrimeNum())
	}
}

func ComputeAverage(c calculator_pb.CalculatorServiceClient) {

	fmt.Println("Starting Client Side Streaming over gRPC ....")

	stream, err := c.ComputeAverage(context.Background())
	if err != nil {
		log.Fatalf("error occured while performing client-side streaming : %v", err)
	}

	requests := []*calculator_pb.ComputeAverageRequest{
		{
			Num: 10,
		},
		{
			Num: 16,
		},
		{
			Num: 20,
		},
		{
			Num: 14,
		},
	}

	for _, req := range requests {
		log.Println("Sending Request.... : ", req)
		stream.Send(req)
		time.Sleep(1 * time.Second)
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("Error while receiving response from server : %v", err)
	}
	log.Println("Response From Server, Average: ", resp.GetAvg())
}

func FindMaxNumber(c calculator_pb.CalculatorServiceClient) {
	fmt.Println("Starting Bi-directional stream by calling Find Max Number over gRPC")

	requests := []*calculator_pb.FindMaxNumberRequest{
		{
			Num: 1,
		},
		{
			Num: 3,
		},
		{
			Num: 5,
		},
		{
			Num: 4,
		},
		{
			Num: 8,
		},
	}

	stream, err := c.FindMaxNumber(context.Background())
	if err != nil {
		log.Fatalf("error occured while performing client side streaming : %v", err)
	}

	waitchan := make(chan struct{})

	go func(requests []*calculator_pb.FindMaxNumberRequest) {
		for _, req := range requests {

			log.Println("Sending Request..... : ", req.GetNum())
			err := stream.Send(req)
			if err != nil {
				log.Fatalf("error while sending request to CalculatorEveryone service : %v", err)
			}
			time.Sleep(1000 * time.Millisecond)
		}
		stream.CloseSend()
	}(requests)

	go func() {
		for {

			resp, err := stream.Recv()
			if err == io.EOF {
				close(waitchan)
				return
			}

			if err != nil {
				log.Fatalf("error receiving response from server : %v", err)
			}

			log.Printf("Response From Server, Max : %v\n", resp.GetMax())
		}
	}()

	<-waitchan
}
