#importing libraries

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
import concurrent.futures


# This class is for Video downloading 
class CineVideoDownloader:
    def __init__(self, urls_file) -> None:
        self.urls_file = urls_file
        self.video_urls =self.read_videos() 
        self.semaphore= threading.Semaphore(5)
        logging.basicConfig(filename='download_log.txt', level=logging.INFO, format='%(message)s')

    # Read URLs from the file
    def read_videos(self):
        with open(self.urls_file, 'r') as file:
            urls =file.readlines()
        video_urls =[url.strip() for url in urls]
        return video_urls
    
    #this method helps in downloading videos
    def download_videos(self, urls):
        link =YouTube(urls)
        stream= link.streams.get_highest_resolution()
        print(f"Downloading video: {link.title}")
        stream.download(output_path="Video_output")
        print(f"Download completed: {link.title}")


    # download and log (calls download methods and logs video info)
    def download_and_log(self, urls):
        with self.semaphore:
            self.download_videos(urls)
            #print("completed download and Log")
            logging.info(f'"Timestamp": {time.strftime("%H:%M, %d %b %Y")}, "URL":"{urls}", "Download":True')
            #print("Logging completed")

    # Download videos serially 
    def download_videos_serial(self):
        start=time.perf_counter()
        for url in self.video_urls:
            self.download_and_log(url)
        end=time.perf_counter()
        print(f'Serial: {end - start} second(s)')


    #Download videos in parallel
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


#Videos processes class that is use for differnt analysis methods-----
class CineVideoProcessor(CineVideoDownloader):
    '''Calling Super class to get all the functionality of CineVideoDownloader'''
    def __init__(self, urls_file):
            super().__init__(urls_file)
            self.nlp = spacy.load('en_core_web_sm')
            nltk.download('punkt')

    #this method helps in extracting audio from video
    def audio_extract(self, video_path,output_path='Audios'):
        Path(output_path).mkdir(parents=True, exist_ok=True)
        video =mp.VideoFileClip(video_path)
        audio_path=os.path.join(output_path, os.path.basename(video_path).replace('.mp4', '.wav'))
        video.audio.write_audiofile(audio_path)
        return audio_path  
    
    # This method helps in transcribing audio to text
    def audio_transcribe(self,audio_path):
        recognizer =sr.Recognizer()
        with sr.AudioFile(audio_path) as source:
            audio=recognizer.record(source)
            text= recognizer.recognize_google(audio)
            return text
    
    # This method helps in performing sentiment analysis on the text
    def sentiment_analysis(self,text,output_path='Sentiments.txt'):
        blob =TextBlob(text)
        with open(output_path, 'a') as file:
            file.write(f"Sentiment: {sentiment}Polarity: {blob.sentiment.polarity} subjectivity: {blob.sentiment.subjectivity}\n")

        return blob.sentiment


    # This method helps in translating text to Spanish

    def transalte_text(self,text):
        translator = Translator()
        translated = translator.translate(text, src='en', dest='es')
        #print(translated.text)
        return translated.text

    

    # This method helps in extracting emotions from the text
    def extract_emotions(self,text, output_path='Emotions.txt'): 
        doc = self.nlp(text)
        full_text = ' '.join([sent.text for sent in doc.sents])
        emotion = NRCLex(full_text)
        #print("Detected Emotions and Frequencies:")
        #print(emotion.affect_frequencies)
        with open(output_path, 'a') as file:
            file.write(f"Detected emotions and frequency: {emotion.affect_frequencies}\n")

        return emotion.affect_frequencies  

#this method processes the video by calling other methods (extract, transcribe, sentiment, translate, and extract emotions)
    def process_videos(self):
        for vidoe_url in os.listdir('Video_output'):
            if vidoe_url.endswith('.mp4'):
                videoPath =os.path.join('Video_output',vidoe_url)
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

#Serial Audio extraction
    def extract_audioSerial(self):
        start =time.perf_counter()
        for vidoe_url in os.listdir('Video_output'):
            if vidoe_url.endswith('.mp4'):
                self.audio_extract(os.path.join('Video_output', vidoe_url))
        end =time.perf_counter()
        print(f'Serial Audio extraction: {end - start} second(s)')

#Parallel audio extraction using Threads
    def extract_audioThreads(self):
        start = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            video_files = [os.path.join('Video_output',file)for file in os.listdir('Video_output') if file.endswith('.mp4')]
            executor.map(self.audio_extract,video_files)
        end =time.perf_counter()
        print(f'Parallel Audio extraction (threads): {end - start} second(s)')

#Parallel processes audio extraction  
    def extract_audioProcesses(self):
        start = time.perf_counter()
        with concurrent.futures.ProcessPoolExecutor() as executor:
            video_files=[os.path.join('Video_output', file)for file in os.listdir('Video_output')if file.endswith('.mp4')]
            executor.map(self.audio_extract,video_files)
        end = time.perf_counter()
        print(f'Parallel audio extraction (processes): {end - start} second(s)')



# Main function to download and process videos
if __name__ == '__main__':
    video_processor = CineVideoProcessor('Video_urls.txt')

    #Step-1 Donload videos

    #Serial
    print("Starting serial download..........")
    video_processor.download_videos_serial()
    print("Serial downloading  completed")

    #Parallel
    print("Starting parallel download......")
    video_processor.download_videos_parallel()
    print("parallel downloading  completed.........")

    #Step-2 Process Vidoes
    video_processor.process_videos()

    #step-3 Comparing audio extractions method
    #Serial extraction
    print("Starting serial audio extraction........")
    video_processor.extract_audioSerial()
    print(' completed serial audio extraction...........')

    #thread extraction
    print("Starting threaded audio extraction......")
    video_processor.extract_audioThreads()
    print("completed threaded audio extraction......")

    #Process extraction
    
    print("Starting process based audio extraction...")
    video_processor.extract_audioProcesses()
    print("Compelted process based audio extraction...")
    