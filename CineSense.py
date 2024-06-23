# Read URL from file.....
import threading
import time
import logging
from pytube import YouTube
from pathlib import Path
import moviepy.editor as mp
import os
import speech_recognition as sr
from textblob import TextBlob
from nrclex import NRCLex
import spacy,nltk
from googletrans import Translator

#Video downloaded class 
class CineVideoDownloader:
    def __init__(self, urls_file) -> None:
        self.urls_file = urls_file
        self.video_urls =self.read_videos() 
        self.semaphore= threading.Semaphore(5)
        logging.basicConfig(filename='download_log.txt', level=logging.INFO, format='%(message)s')

    #Read URLS
    def read_videos(self):
        with open(self.urls_file, 'r') as file:
            urls =file.readlines()
        video_urls =[url.strip() for url in urls]
        return video_urls
    
    #Download video ( It will help in downloading videos)
    def download_videos(self, urls):
        link =YouTube(urls)
        stream= link.streams.get_highest_resolution()
        print(f"Downloading video: {link.title}")
        stream.download(output_path="Videos_output")
        print(f"Download completed: {link.title}")


    #download_&_log(calling download medthods and  log videos info)
    def download_and_log(self, urls):
        with self.semaphore:
            self.download_videos(urls)
            print("completed download and Log")
            logging.info(f'"Timestamp": {time.strftime("%H:%M, %d %b %Y")}, "URL":"{urls}", "Download":True')
            print("Logging completed")

    #download videos serially 
    def download_videos_serial(self):
        start=time.perf_counter()
        for url in self.video_urls:
            self.download_and_log(url)
        end=time.perf_counter()
        print(f'Serial: {end - start} second(s)')


    #Download videos parllely
    def download_videos_parallel(self):
        start=time.perf_counter()
        threads= []
        for url in self.video_urls:
            thread =threading.Thread(target=self.download_and_log, args=(url,))
            threads.append(thread)
            thread.start()
        for thread in threads:
            thread.join()
        end = time.perf_counter()
        print(f'Parallel: {end - start} second(s)')


#Videos processes class inherit download class-----
class CineVideoProcessor(CineVideoDownloader):
    def __init__(self, urls_file):
            super().__init__(urls_file)
            self.nlp = spacy.load('en_core_web_sm')
            nltk.download('punkt')

    def audio_extract(self, video_path,output_path='Audio'):
        Path(output_path).mkdir(parents=True, exist_ok=True)
        video =mp.VideoFileClip(video_path)
        audio_path=os.path.join(output_path, os.path.basename(video_path).replace('.mp4', '.wav'))
        video.audio.write_audiofile(audio_path)
        return audio_path  
    
    def audio_transcribe(self,audio_path):
        recognizer =sr.Recognizer()
        with sr.AudioFile(audio_path) as source:
            audio=recognizer.record(source)
            text= recognizer.recognize_google(audio)
            return text
    

    def sentiment_analysis(self,text):
        blob =TextBlob(text)
        return blob.sentiment


    def transalte_text(self,text):
        translator = Translator()
        translated = translator.translate(text, src='en', dest='es')
        #print(translated.text)
        return translated.text



    def extract_emotions(self,text): 
        doc = self.nlp(text)
        full_text = ' '.join([sent.text for sent in doc.sents])
        emotion = NRCLex(full_text)
        #print("Detected Emotions and Frequencies:")
        #print(emotion.affect_frequencies)
        return emotion.affect_frequencies  

    def process_videos(self):
        for vidoe_url in os.listdir('Videos_output'):
            if vidoe_url.endswith('.mp4'):
                videoPath =os.path.join('Videos_output',vidoe_url)
                audioPath =self.audio_extract(videoPath)

                text =self.audio_transcribe(audioPath)
                transcript_path = os.path.join('Transcripts', os.path.basename(audioPath).replace('.wav', '.txt'))
                Path('Transcripts').mkdir(parents=True, exist_ok=True)
                with open(transcript_path,'w') as file:
                    file.write(text)

                sentiment =self.sentiment_analysis(text)
                print(f'Sentiment for {vidoe_url}: {sentiment}')

                translated_text = self.transalte_text(text)
                translation_path=os.path.join('Translations', os.path.basename(audioPath).replace('.wav','.txt'))
                Path('Translations').mkdir(parents=True, exist_ok=True)
                with open(translation_path,'w') as file:
                    file.write(translated_text)

                emotions =self.extract_emotions(text)
                print(f'Emotions for {vidoe_url} : \n {emotions}')






    #download videos
if __name__ == '__main__':
    video_processor = CineVideoProcessor('Video_urls.txt')
    video_processor.download_videos_serial()
    video_processor.process_videos()
