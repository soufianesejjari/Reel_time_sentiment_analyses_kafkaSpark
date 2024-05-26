import os
import time
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from bs4 import BeautifulSoup
import pandas as pd
from kafka import KafkaProducer
import json
from datetime import datetime

class HespressProducer:
    def __init__(self, server):
        self.server = server
        self.bootstrap_servers = server
        self.topic = 'test-sentiments'
        self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers)

    def hespress_articles(self, mots, rep=20):
        driver = webdriver.Chrome('C:/Users/sejja/chromedriver')
        link = "https://en.hespress.com/?s=" + mots
        driver.get(link)

        # Determine the rep enough to scroll all the page
        last_height = driver.execute_script("return document.body.scrollHeight")

        for i in range(rep):
            driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')
            time.sleep(1)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height  # Update last_height

        src = driver.page_source
        soup = BeautifulSoup(src, 'lxml')

        uls = soup.find('div', {'class': 'search_results row'})
        data = []
        for div in uls.findAll('div', {'class': 'col-12 col-sm-6 col-md-4 col-xl-3'}):
            try:
                title = div.find('h3', {'class': 'card-title'}).text.strip()
                link = div.find('a', {'class': 'stretched-link'}).get('href')
                date = div.find('small', {'class': 'text-muted time'}).text.strip()
                data.append({'Title': title, 'Link': link, 'Date': date})
            except AttributeError:
                pass
        for div in uls.findAll('div', {'class': 'col-12 col-sm-6 col-md-6 col-xl-4 px-2'}):
            try:
                title = div.find('h3', {'class': 'card-title'}).text.strip()
                link = div.find('a', {'class': 'stretched-link'}).get('href')
                date = div.find('small', {'class': 'text-muted time'}).text.strip()
                data.append({'Title': title, 'Link': link, 'Date': date})
            except AttributeError:
                pass
        df = pd.DataFrame(data)
        driver.close()
        driver.quit()
        return df

    def extract_data(self, url):
        try:
            driver = webdriver.Chrome('C:/Users/sejja/chromedriver')
            driver.get(url)
            time.sleep(1)

            src = driver.page_source
            soup = BeautifulSoup(src, 'lxml')

            titre = soup.find('h1', {'class': 'post-title'})
            titre = titre.get_text().strip() if titre else "not available"

            date = soup.find('span', {'class': 'date-post'})
            date = date.get_text().strip() if date else "not available"

            sections = soup.find('section', {'class': 'box-tags'})
            tags = ""
            if sections:
                tags = ", ".join([section.get_text().strip() for section in sections.findAll('a')])

            article = soup.find('div', {'class': 'article-content'})
            text = "\n".join([p.get_text().strip() for p in article.findAll('p')])

            comments_area = soup.find('ul', {'class': 'comment-list hide-comments'})
            comments = []
            if comments_area:
                for comment in comments_area.findAll('li', {'class': 'comment even thread-even depth-1 not-reply'}):
                    comment_date = comment.find('div', {'class': 'comment-date'})
                    comment_content = comment.find('div', {'class': 'comment-text'})
                    comment_react = comment.find('span', {'class': 'comment-recat-number'})
                    if comment_date and comment_content and comment_react:
                        comments.append({
                            "comment_date": comment_date.get_text(),
                            "comment_content": comment_content.get_text(),
                            "comment_react": comment_react.get_text()
                        })

            return {'Date': date, 'Titre': titre, 'Tags': tags, 'content': text, 'Comments': comments}
        except (NoSuchElementException, TimeoutException) as e:
            print(f"Error encountered while processing {url}: {e}")
            return None
        finally:
            driver.quit()

    def run(self, keyword):
        result_df = pd.DataFrame(columns=['Date', 'Titre', 'Tags', 'Comments'])
        for d in self.hespress_articles(keyword)['Link']:
            extracted_data = self.extract_data(d)
            if extracted_data:
                result_df = result_df.append(extracted_data, ignore_index=True)
                result_df['Date'] = pd.to_datetime(result_df['Date'], errors='coerce')
                result_df = result_df.dropna(subset=['Date'])
                result_df['Date'] = result_df['Date'].dt.strftime("%Y-%m-%d %H:%M:%S")

                message = {
                    'source': 'Hespress',
                    'date': result_df['Date'].iloc[-1],  # Assuming you want the most recent entry's date
                    'videoId': d,
                    'comment': result_df['Titre'].iloc[-1],  # Assuming you want the most recent entry's title
                    'topic': keyword
                }
                print(f"Sent message: {message}")
                self.producer.send(self.topic, value=json.dumps(message).encode('utf-8'))
                time.sleep(40)

