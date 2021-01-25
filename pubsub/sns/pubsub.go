package pubsub

import (
	"context"
	"encoding/json"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/google/uuid"
)

type PubSub struct {
	sns *sns.SNS
	sqs *sqs.SQS
}

func CreatePubSub() *PubSub {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	sns := sns.New(sess)
	sqs := sqs.New(sess)
	return &PubSub{
		sns: sns,
		sqs: sqs,
	}
}

type message struct {
	ID      string `json:"id"`
	Payload string `json:"payload"`
}

func (p *PubSub) Publish(ctx context.Context, id uuid.UUID, payload []byte) error {
	msg := message{
		ID:      id.String(),
		Payload: string(payload),
	}
	data, _ := json.Marshal(msg)
	requestID := ctx.Value("requestID").(string)

	publishInput := &sns.PublishInput{
		MessageAttributes: map[string]*sns.MessageAttributeValue{
			"requestID": &sns.MessageAttributeValue{
				StringValue: aws.String(requestID),
			},
		},
		Message:        aws.String(string(data)),
		MessageGroupId: aws.String(requestID),
		TopicArn:       aws.String(os.Getenv("SNS_RESULT_ARN")),
	}
	if _, err := p.sns.Publish(publishInput); err != nil {
		panic(err)
	}
	return nil
}

func (p *PubSub) Subscribe(ctx context.Context, id uuid.UUID) []byte {
	dataCH := make(chan []byte)
	requestID := ctx.Value("requestID").(string)

	go func() {
	Loop:
		for {
			msgResult, err := p.sqs.ReceiveMessage(&sqs.ReceiveMessageInput{
				AttributeNames: []*string{
					aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
				},
				MessageAttributeNames: []*string{
					aws.String(sqs.QueueAttributeNameAll),
				},
				QueueUrl:            aws.String(os.Getenv("SQS_RESULT_URL")),
				MaxNumberOfMessages: aws.Int64(10),
				VisibilityTimeout:   aws.Int64(100),
				WaitTimeSeconds:     aws.Int64(1),
			})
			if err != nil {
				panic(err)
			}

			for _, msg := range msgResult.Messages {
				msgRequestID := msg.MessageAttributes["requestID"].String()
				if msgRequestID == requestID {
					rawMessage := map[string]interface{}{}
					json.Unmarshal([]byte(*msg.Body), &rawMessage)

					var parsed message
					json.Unmarshal([]byte(rawMessage["Message"].(string)), &parsed)
					p.sqs.DeleteMessage(&sqs.DeleteMessageInput{
						ReceiptHandle: msg.ReceiptHandle,
					})
					dataCH <- []byte(parsed.Payload)
					break Loop
				}
			}
		}
	}()
	return <-dataCH
}
