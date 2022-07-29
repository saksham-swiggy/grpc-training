package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"time"

	calculator_pb "github.com/saksham-swiggy/grpc-training/calculator_pb/gen"
	"google.golang.org/grpc"
)

type server struct {
	calculator_pb.UnimplementedCalculatorServiceServer
}

func (*server) Sum(ctx context.Context, req *calculator_pb.SumRequest) (resp *calculator_pb.SumResponse, err error) {

	fmt.Println("Sum Function invoked to demonstrate unary communication")

	num1 := req.GetNum1()
	num2 := req.GetNum2()

	resp = &calculator_pb.SumResponse{
		Sum: num1 + num2,
	}
	return resp, nil
}

func (*server) PrimeNumber(req *calculator_pb.PrimeNumberRequest, resp calculator_pb.CalculatorService_PrimeNumberServer) error {

	fmt.Println("Prime Number function invoked for server side streaming")

	isPrime := func(num int64) bool {
		if num <= 1 {
			return false
		}
		limit := int64(math.Sqrt(float64(num)))
		for i := int64(2); i <= limit; i++ {
			if num%i == 0 {
				return false
			}
		}
		return true
	}

	limit := req.GetLimit()

	for i := int64(0); i <= limit; i++ {
		if isPrime(i) {
			res := calculator_pb.PrimeNumberResponse{
				PrimeNum: i,
			}
			time.Sleep(1000 * time.Millisecond)
			resp.Send(&res)
		}
	}
	return nil
}

func (*server) ComputeAverage(stream calculator_pb.CalculatorService_ComputeAverageServer) error {

	fmt.Println("Compute Average Function is invoked to demonstrate client side streaming")

	var avg int64 = 0
	var count int64 = 0

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&calculator_pb.ComputeAverageResponse{
				Avg: avg / count,
			})
		}

		if err != nil {
			log.Fatalf("Error while reading client stream : %v", err)
		}

		num := msg.GetNum()
		count++
		avg += num
	}
}

func (*server) FindMaxNumber(stream calculator_pb.CalculatorService_FindMaxNumberServer) error {

	fmt.Println("Find Max Number invoked to demonstrate Bi-directional streaming")

	var max int64 = math.MinInt64

	for {

		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}

		if err != nil {
			log.Fatalf("error while receiving data from Calculator client : %v", err)
			return err
		}

		num := req.GetNum()

		if num > max {
			max = num
			sendErr := stream.Send(&calculator_pb.FindMaxNumberResponse{
				Max: max,
			})

			if sendErr != nil {
				log.Fatalf("error while sending response to Calculator Client : %v", err)
				return err
			}
		}
	}
}

func main() {

	fmt.Println("Server started")
	listen, err := net.Listen("tcp", "0.0.0.0:8000")
	if err != nil {
		log.Fatalf("Failed to Listen: %v", err)
	}

	s := grpc.NewServer()
	calculator_pb.RegisterCalculatorServiceServer(s, &server{})

	if err = s.Serve(listen); err != nil {
		log.Fatalf("failed to serve : %v", err)
	}
}
