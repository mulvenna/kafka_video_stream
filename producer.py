import time
import cv2
from kafka import KafkaProducer
# connect to Kafka
producer = KafkaProducer(bootstrap_servers='0.0.0.0:9092')
# assign a topic
topic = 'my-topic'

def video_emitter(video):
    # open the video
    video = cv2.VideoCapture(video)
    print(' emitting.....')
    # read the file
    while (video.isOpened):
        # read the image in each frame
        success, image = video.read()
        # check if the file has read to the end
        if not success:
            break
        # convert the image png
        ret, jpeg = cv2.imencode('.png', image)
        # convert the image to bytes and send to kafka
        producer.send(topic, jpeg.tobytes())
        # to reduce CPU usage create sleep time of 0.2sec
        time.sleep(0.01)
    # clear the capture
    video.release()
    print('done emitting')

if __name__ == '__main__':
    video_emitter('timelapse.mp4')
