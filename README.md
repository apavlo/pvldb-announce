# PVLDB Paper Announcement

* *Twitter:* https://twitter.com/pvldb
* *RSS:* https://db.cs.cmu.edu/files/rss/pvldb-rss.xml
* *Atom:* https://db.cs.cmu.edu/files/rss/pvldb-atom.xml

This is a Python script that scrapes the PVLDB website and extracts new paper announcements.
It can then post them to a RSS/Atom file and announcement them on Twitter.

## Usage

* **Collect papers**
    ```bash
    python ./pvldb-announce.py --collect $PATH_TO_SQLITE_DB
    ```

* **Create RSS/Atom files**
    ```bash
    python ./pvldb-announce.py --rss \
        --rss-path=$PATH_TO_STORE_RSS_FILES \
        $PATH_TO_SQLITE_DB
    ```

* **Upload to twitter**
    ```bash
    python ./pvldb-announce.py --twitter \
        --twitter-consumer-key=$TWITTER_CONSUMER_KEY \
        --twitter-consumer-secret=$TWITTER_CONSUMER_SECRET \
        --twitter-access-token=$TWITTER_ACCESS_TOKEN \
        --twitter-access-secret=$TWITTER_ACCESS_SECRET \
        $PATH_TO_SQLITE_DB
    ```
