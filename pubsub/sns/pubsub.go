package pubsub

import (
	"encoding/json"
	"os"
	"strings"

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
	Payload []byte `json:"payload"`
}

func (p *PubSub) Publish(id uuid.UUID, payload []byte) error {
	msg := message{
		ID:      id.String(),
		Payload: payload,
	}
	data, _ := json.Marshal(msg)
	publishInput := &sns.PublishInput{
		Message:  aws.String(string(data)),
		TopicArn: aws.String(os.Getenv("SNS_RESULT_ARN")),
	}
	if _, err := p.sns.Publish(publishInput); err != nil {
		panic(err)
	}
	return nil
}

func (p *PubSub) Subscribe(id uuid.UUID) []byte {
	dataCH := make(chan []byte)

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
				QueueUrl:            aws.String("SQS_RESULT_URL"),
				MaxNumberOfMessages: aws.Int64(10),
				VisibilityTimeout:   aws.Int64(100),
				WaitTimeSeconds:     aws.Int64(1),
			})
			if err != nil {
				panic(err)
			}

			for _, msg := range msgResult.Messages {
				if strings.Contains(*msg.Body, id.String()) {
					var parsed message
					str := *msg.Body
					json.Unmarshal([]byte(str), &parsed)
					p.sqs.DeleteMessage(&sqs.DeleteMessageInput{
						ReceiptHandle: msg.ReceiptHandle,
					})
					dataCH <- parsed.Payload
					break Loop
				}
			}
		}
	}()
	return <-dataCH
}
