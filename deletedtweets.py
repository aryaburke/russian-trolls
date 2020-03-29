

import sqlite3
import csv
import string

def initialize(db):
    c = db.cursor()
    q = """CREATE TABLE IF NOT EXISTS Users(
        id INT(30) PRIMARY KEY NOT NULL,
        name VARCHAR(30),
        screen_name VARCHAR(60),
        description TINYTEXT,
        location VARCHAR(30),
        time_zone VARCHAR(30),
        lang VARCHAR(30),
        verified VARCHAR(5),
        created_at VARCHAR(30),
        followers_count INT,
        statuses_count INT,
        favorites_count INT,
        friends_count INT,
        listed_count INT        
    )"""
    c.execute(q)
    q = """CREATE TABLE IF NOT EXISTS Tweets(
        user_id INT(30),
        tweet_id INT(50) PRIMARY KEY,
        text TINYTEXT,
        date VARCHAR(10),
        created_str VARCHAR(30),
        source VARCHAR(100),
        retweeted VARCHAR(5),
        posted VARCHAR(10),
        favorite_count INT(10),
        retweet_count INT(10),
        retweeted_status_id INT(50),
        in_reply_to_status_id INT(50),
        FOREIGN KEY (user_id) REFERENCES Users(id)
    )"""
    c.execute(q)
    q ="""CREATE TABLE IF NOT EXISTS Hashtags(
        tweet_id INT(50),
        tag VARCHAR(149),
        date VARCHAR(10),
        FOREIGN KEY (tweet_id) REFERENCES Tweets(tweet_id)
    )"""
    c.execute(q)
    q ="""CREATE TABLE IF NOT EXISTS Hashtag_Frequency(
        tag VARCHAR(149) PRIMARY KEY,
        frequency INT(50),
        FOREIGN KEY (tag) REFERENCES Hashtags(tag)
    )"""
    c.execute(q)
    q = """CREATE TABLE IF NOT EXISTS Mentions(
        tweet_id INT(50),
        mentioned_id INT(30),
        FOREIGN KEY (tweet_id) REFERENCES Tweets(tweet_id)
    )"""
    c.execute(q)
    q = """CREATE TABLE IF NOT EXISTS Expanded_URLs(
        tweet_id INT(50),
        url VARCHAR(150),
        FOREIGN KEY (tweet_id) REFERENCES Tweets(tweet_id)
    )"""
    c.execute(q)
    q = """CREATE INDEX idx_tweet_users ON Tweets (user_id);
    CREATE INDEX idx_hashtags ON Hashtags (tweet_id, tag, date);
    CREATE INDEX idx_frequency ON Hashtag_Frequency (frequency);
    CREATE INDEX idx_mentions ON Mentions (tweet_id);
    CREATE INDEX idx_urls ON Expanded_URLs (tweet_id)"""
    c.execute(q)
    c.close()
    
"""
users.csv stores rows as:
"id","location","name","followers_count","statuses_count","time_zone","verified","lang",
"screen_name","description","created_at","favourites_count","friends_count","listed_count"
"""
def import_users(db):
    c = db.cursor()
    with open('users.csv') as users:
        reader = csv.reader(users)
        for row in reader:
            idnum = row[0]
            name = row[2]
            screen_name = row[8]
            description = row[9]
            location = row[1]
            time_zone = row[5]
            lang = row[7]
            verified = row[6]
            created_at = row[10]
            followers_count = row[3]
            statuses_count = row[4]
            favorites_count = row[11]
            friends_count = row[12]
            listed_count = row[13]
            g = (idnum, name, screen_name, description, location, time_zone, 
                 lang, verified, created_at, followers_count, statuses_count,
                 favorites_count, friends_count, listed_count)
            if idnum != '':
                add(c, "Users", "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", g)
    db.commit()
    c.close()   
            
"""
tweets.csv stores rows as:
"user_id","user_key","created_at","created_str","retweet_count","retweeted","favorite_count",
"text","tweet_id","source","hashtags","expanded_urls","posted","mentions","retweeted_status_id",
"in_reply_to_status_id"
"""
def import_tweets(db):        
    c = db.cursor()
    tagc = db.cursor()
    with open('tweets.csv') as tweets:
        reader = csv.reader(tweets)
        for row in reader:
            user_id = row[0]
            created_str = row[3]
            retweet_count = row[4]
            retweeted = row[5]
            favorite_count = row[6]
            text = row[7].replace("'", "%%%%%")
            tweet_id = row[8]
            source = row[9]
            posted = row[12]
            retweeted_status_id = row[14]
            in_reply_to_status_id = row[15]
            if user_id == 'user_id':
                date = "date"
            else:
                date = created_str[0:10]
            if user_id != '' and tweet_id != '':
                add(c, "Tweets", "(?,?,?,?,?,?,?,?,?,?,?,?)", (user_id, tweet_id, text, date, created_str, source, retweeted, posted, favorite_count, retweet_count, retweeted_status_id, in_reply_to_status_id))
            hashtags = hashtagparse(row[7])
            for tag in hashtags:
                add(c, "Hashtags", "(?,?,?)", (tweet_id, tag, date))
                q = "SELECT tag FROM Hashtag_Frequency WHERE tag=?"
                tagc.execute(q, (tag,))
                x = tagc.fetchone()
                if x != None:
                    q = "UPDATE Hashtag_Frequency SET frequency = frequency + 1 WHERE tag=?"
                    tagc.execute(q, (tag,))
                else:
                    q = "INSERT INTO Hashtag_Frequency VALUES (?,?)"
                    tagc.execute(q, (tag, 1))
            urls = stringlist_to_list(row[11])
            for url in urls:
                if url != "":
                    add(c, "Expanded_URLs", "(?,?)", (tweet_id, url))
            mentions = stringlist_to_list(row[13])
            for mentioned_id in mentions:
                if mentioned_id != "":
                    add(c, "Mentions",  "(?,?)", (tweet_id, mentioned_id))
    db.commit()
    c.close()
    tagc.close()
    
def reset(db):
    cursor = db.cursor()
    q = "DELETE FROM Users"
    cursor.execute(q)
    q = "DELETE FROM Tweets"
    cursor.execute(q)
    q = "DELETE FROM Hashtags"
    cursor.execute(q)
    q = "DELETE FROM Mentions"
    cursor.execute(q)
    q = "DELETE FROM Expanded_URLs"
    cursor.execute(q)
    q = "DELETE FROM Hashtag_Frequency"
    cursor.execute(q)
    db.commit()
    cursor.close()

    
def add(cursor, relation, values_string, d):
    """Returns true if n did not previously exist and inserts into the table. 
    Otherwise returns false. Assumes cursor is a valid cursor
    into database.
    """
    q = "INSERT INTO {} VALUES {}".format(relation, values_string)
    try:
        cursor.execute(q, d)
    except sqlite3.IntegrityError as e:
        print(e)
        return False
    #except sqlite3.OperationalError as m:
    #    db.close() #try except as a close
    else:
        return True

#made this because the data of hashtags is impure/inaccurate, returns list of #s
def hashtagparse(text):
    wordlist = text.split()
    hashtags = []
    for w in wordlist:
        if w[0] == '#':
            x = w.strip(string.punctuation)
            x = x.lower()
            x = "#" + x
            hashtags.append(x)
    return hashtags
    
def stringlist_to_list(text):
    text = text.strip("[]")
    text = text.split(",")
    strippedtext = []
    for _ in text:
         strippedtext.append(_.strip('"'))
    return strippedtext


#gets the most popular hashtags from the Hashtag_Frequency table (does not work with dates)
def get_most_popular_hashtags(db, n):
    c = db.cursor()
    q = "SELECT * FROM Hashtag_Frequency ORDER BY frequency DESC"
    c.execute(q)
    tags = []
    for i in range(n):
        x = c.fetchone()
        tags.append(x)
    return tags
      
"""        
TABLE Tweets
        0user_id INT(30),
        1tweet_id INT(50) PRIMARY KEY,
        2text TINYTEXT,
        3date VARCHAR(10),
        4created_str VARCHAR(30),
        5source VARCHAR(100),
        6retweeted VARCHAR(5),
        7posted VARCHAR(10),
        8favorite_count INT(10),
        9retweet_count INT(10),
        10retweeted_status_id INT(50),
        11in_reply_to_status_id INT(50),
"""

#returns dictionary with the tweet, favorite, and retweet count of each date.
def mine_dates(db):
    c = db.cursor()
    #do we need *?
    q = "SELECT * FROM Tweets"
    c.execute(q)
    #store in bag with format {date:{t_count:, fav_count:, rt_count:}}
    bag = {}
    for row in c:
        date = row[3]
        fav_count = row[8]
        rt_count = row[9]
        if fav_count == '':
            fav_count = 0
        if rt_count == '':
            rt_count = 0
        if date != '' and date != 'date':
            if date in bag:
                bag[date]['t_count'] += 1
                bag[date]['fav_count'] += fav_count
                bag[date]['rt_count'] += rt_count
            else:
                bag[date] = {'t_count':1, 'fav_count':fav_count, 'rt_count':rt_count}
    return bag

#returns num_of_dates dates with the most interactions with tweets (tweets, favorites, retweets)
def get_top_dates(db, num_of_dates=5, num_of_tags=5):
    dates = mine_dates(db)
    l = []
    for _ in range(num_of_dates):
        l.append((0,"date"))
    for date, counts in dates.items():
        total = counts['t_count']+counts['fav_count']+counts['rt_count']
        i = 0
        while i < len(l) and total < l[i][0] :       
            i += 1
        if i < len(l):
            l = l[0:i] + [(total, date)] + l[i:len(l)-1]
    return l

#returns a list of the top hashtags for a given date
def get_top_tags_for_date(db, date, num_of_tags): 
    c = db.cursor()
    q = "SELECT tag FROM Hashtags WHERE date = ?"
    c.execute(q, (date,))
    bag = {}
    for tag in c:
        if tag[0] in bag:
            bag[tag[0]] += 1
        else:
            bag[tag[0]] = 1
    sort = sorted(bag.items(), key=lambda x: x[1], reverse=True)
    c.close()
    return sort[:num_of_tags]
 
#returns the total number of tweets that are retweets in the database
def total_retweets(db):
    c = db.cursor()
    q = 'SELECT COUNT(retweeted_status_id) FROM Tweets WHERE retweeted_status_id <> ""'
    c.execute(q)
    total_rts = c.fetchone()[0]
    c.close()
    return total_rts

#returns the total number of retweets that are retweets of propaganda bots in the database
def propaganda_retweets(db):
    c = db.cursor()
    q = "SELECT COUNT(A.tweet_id) FROM Tweets AS A, Tweets as B WHERE A.retweeted_status_id = B.tweet_id"
    c.execute(q)
    propaganda_rts = c.fetchone()[0]
    c.close()
    return propaganda_rts

#for all retweets that are bots retweeting bots, finds how many of those retweets are
#'megaphone' retweets. A megaphone retweet is defined as: a retweet has more favorites/retweets than its original
def megaphone_retweets(db):
    c = db.cursor()
    q = "SELECT COUNT(A.tweet_id) FROM Tweets AS A, Tweets as B WHERE A.retweeted_status_id = B.tweet_id AND A.favorite_count + A.retweet_count > B.favorite_count + B.retweet_count"
    c.execute(q)
    r  = c.fetchone()[0]
    c.close()
    return r

#returns the slope of the best-fit line for all users' x=followers_count, y=statuses_count
def best_fit(db):
    c = db.cursor()
    q = 'SELECT SUM(followers_count), SUM(statuses_count) FROM Users WHERE followers_count <> "" AND statuses_count <> ""'
    c.execute(q)
    x = c.fetchone()
    return (x[0] / x[1])


def most_influential_tweets(db, n):
    c = db.cursor()
    q = "SELECT tweet_id, favorite_count, retweet_count, retweeted_status_id FROM Tweets WHERE favorite_count + retweet_count > 500"
    c.execute(q)
    scag = {}
    for tweet in c:
        #adds count to original tweet id if it's a retweet
        if tweet[3] == "":
            if tweet[0] in scag:
                scag[tweet[0]] += (int(tweet[1]) + int(tweet[2]))
            else:
                scag[tweet[0]] = (int(tweet[1]) + int(tweet[2]))
        else:
            if tweet[3] in scag:
                scag[tweet[3]] += (int(tweet[1]) + int(tweet[2]))
            else:
                scag[tweet[3]] = (int(tweet[1]) + int(tweet[2]))
    sort = sorted(scag.items(), key=lambda x: x[1], reverse=True)
    #sort is a list of tuples of (tweet_id, engagements) sorted from most to least
    tweets = []
    for i in range(n):
        q = "SELECT name, screen_name, text, created_str FROM Tweets JOIN Users ON user_id = id WHERE tweet_id = ?"
        c.execute(q, (sort[i][0],))
        t = c.fetchone()
        #creates list of tuples (engagements, name, screen_name, created_str, text)
        tweets.append((sort[i][1], t[0], t[1], t[3], t[2]))
    return tweets
    
#returns a text month for a number of a month
def date_to_month(d):
    months=["January","February","March","April","May","June","July","August", "September","October","November","December"]
    return months[int(d)-1]

#example: 2017-02-27 14:54:00
def created_str_to_full_date(s):
    year = s[:4]
    month = date_to_month(s[5:7])
    day = s[8:10]
    hour = s[11:13]
    minute = s[14:16]
    return "{} {}, {} at {}:{}".format(month, day, year, hour, minute)
    
def text_unparse(s):
    i = 5
    if len(s) > 5:
        while i <= len(s):
            if s[i-5:i] == "%%%%%": 
                s = s[:i-5] + "'" + s[i:]
            i += 1
    elif s == "%%%%%":
        s = "'"
    return s

MENU = """
Welcome to the #RussiaGate database. Here we have stored data on over 3 million tweets of Russian propaganda accounts that were deleted by Twitter. These accounts were active starting in 2014. With this tool we have the ability to meta-analyze much of this data.

1) Get most popular hashtags
2) Get most active dates and their top hashtags
3) Get the most influential tweets
4) Find the percentage of total retweets propaganda retweets (bots retweeting other bots)
5) Find the percentage of propaganda retweets that are megaphone retweets (the retweet has more engagements than the original))
6) Find the slope of the line of best-fit to the graph of all users' x = followers, y = tweets. If we theorize that users are used either as followers/engagers or larger influencers, the line of best-fit would have a slope of around 1.
7) Reload database            
8) Reprint menu
9) Quit
"""

TWEET_DESC = """\n{} engagements
{} (@{})
{}
{}
"""


if __name__ == '__main__':
    db = sqlite3.connect('deleted_tweets.db')
    repl_on = True
    print(MENU)
    while repl_on:
        action = input('\nSelect menu option: ')
        
        if action == '1':
            n = int(input('\nHow many top hashtags would you like to see? '))
            tags = get_most_popular_hashtags(db, n)
            for _ in tags:
                print('{} used {} times.'.format(_[0],_[1]))
        
        if action == '2':
            n = int(input('\nHow many top dates would you like to see? '))
            z = int(input('\nHow many top hashtags would you like to see for each date? '))
            top = get_top_dates(db,n)
            for d in top: 
                print('\n{} {}, {} had {} interactions.'.format(date_to_month(int(d[1][5:7])),d[1][8:10], d[1][:4],d[0]))
                tags = get_top_tags_for_date(db, str(d[1]), z)
                for _ in tags:
                    print('      {} used {} times.'.format(_[0],_[1]))
        
        if action == '3':
            n = int(input('\nHow many top tweets would you like to see? '))
            tweets = most_influential_tweets(db, n)
            #tweets format: (engagements, name, screen_name, created_str, text)
            for t in tweets:
                print(TWEET_DESC.format(t[0], t[1], t[2], created_str_to_full_date(t[3]), text_unparse(t[4])))
        
        if action == '4':
            x = (propaganda_retweets(db)/total_retweets(db))
            x = str(x)
            x = x[:7]
            print(x + "% of retweets are bots retweeting each other! Our post-discovery analysis is that that's not very many. That said, many bots who were not deleted are likely not in the database. Also, retweeting and engaging with human accounts makes bots seem, as individual accounts, more like human users. It also brings the bot's handle into places where other human users are engaging. In this way, it makes sense that the bots aren't retweeting other bots, especially because lots of their intended messaging already exists within human conservative Twitter, and it is support that needs to be manufactured. Instead, retweets can serve two functions: 1) making a bot seem human and therefore more legitimate and 2) make the bot an active presence in the Twitterverse, allowing other users to find its tweets, page, and ideology.") 
            
        if action == '5':
            foot = (megaphone_retweets(db) / propaganda_retweets(db))
            print(str(foot) + "% of retweets are megaphone retweets. Our post-discovery analysis is that, from a tactical perspective, this makes sense. There's no need to amplify tweets from smaller accounts (that are perhaps primarily used to like and retweet the tweets of 'influencers' to make them trending). We hypothesize that an original tweet gets more engagements than if it were a retweet, so using the megaphone tactic would reduce engagements. Megaphone tweets occur organically between human users, leading us to believe that a simulated organicity was not attempted on a network level. Because of this discovery, we tried to tackle menu item #5.")
        
        if action == '6':
            print(str(best_fit(db))[:9] + " is the slope of the best-fit line. This means that the number of tweets increase at a 1.024:1 ratio with followers, which is nearly 1:1. This proves our hypothesis correct.")
        
        #doubles are doubled in data...
        if action == '7':
            initialize(db)
            reset(db)
            import_users(db)
            import_tweets(db)
            print('Data reloaded!')
        
        if action == '8':
            print(MENU)
            
        if action == '9':
            repl_on = False
    db.close()
    