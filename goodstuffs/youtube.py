#!/bin/env python
# source: https://gist.github.com/benzap/90ff22790bc0a9c6fd2902e91da4baef
# Requires: youtube_dl module
# Requires: ffmpeg
# Usage:
# python youtube.py [<file_url>] [--playlist]
# 
# Example:
# python youtube_list -pl
# 

import youtube_dl
import sys
import os


import argparse
parser = argparse.ArgumentParser(description = 'Download Youtube music as mp3')
parser.add_argument('-f', '--file_urls',  help = 'The filename where you put your urls',default = 'youtube_list')
parser.add_argument('-pl', '--playlist', action='store_true',default = False, help = 'if flag, whole playlist will be downloaded, otherwise only the corresponding video')  
args = parser.parse_args()

noplaylist =  not args.playlist 

os.chdir('/Users/yangj2/Desktop/Personal/Youtube Music/') # the dir where you put your ls_urls
with open(args.file_urls) as f:
    urls = f.readlines()
urls = [url.strip('\n') for url in urls]
print(urls)

ydl_opts = {
    'format': 'bestaudio/best',
    'postprocessors': [{
        'key': 'FFmpegExtractAudio',
        'preferredcodec': 'mp3',
        'preferredquality': '192',
        
    }],
    'noplaylist': noplaylist,
    'outtmpl': '%(title)s.%(ext)s'
}

# Download in music folder
if not os.path.isdir('./music/'):
    os.mkdir('./music/')
os.chdir('./music/')
if __name__ == "__main__":
    with youtube_dl.YoutubeDL(ydl_opts) as ydl:
        ydl.download(urls)


