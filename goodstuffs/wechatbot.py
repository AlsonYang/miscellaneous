#%% can send image, small files, but not large files like mp3 sadly
from wxpy import *
import os

os.chdir("/Users/yangj2/Desktop/Personal/Youtube Music/music/")
print(os.listdir())
print(os.path.join(os.getcwd(), os.listdir()[2]))
bot = Bot(cache_path=True)
# my_friend = bot.friends().search("alson.y", sex=MALE, city="汕头")[0]
# print(my_friend)

# bot.file_helper.send_image(
#     "/Users/yangj2/Desktop/Screenshots/Screenshot2020-04-08at17.27.35.png"
# )

bot.file_helper.send_file(
    "/Users/yangj2/Desktop/Personal/Youtube Music/music/Strongberryherface.mp3"
)

my_friend.send_file(
    path="/Users/yangj2/Desktop/Personal/Youtube Music/music/Strongberry 《 她的模樣 》Lyric Video.mp3"
)
my_friend.send_file(
    path="/Users/yangj2/Desktop/Personal/Youtube Music/music/鹿林號《 答案 The story of my heart 》Lyric Video.mp3"
)
