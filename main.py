from bs4 import BeautifulSoup
import requests
import mysql.connector
import os
from dotenv import load_dotenv
import schedule
import time

load_dotenv()

db_config = {
    'host': os.getenv("HOST"),
    'user': os.getenv("USER"),
    'password': os.getenv("PASSWORD"),
    'database': os.getenv("DATABASE")
}

def parse_website(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    print("Найденные элементы:")
    news_items = []
    for item in soup.find_all('article'):
        print(item)
        title = item.find('h2').text.strip()  # Ищем заголовок по классу
        link = item.find('a', class_='tm-title__link')['href']
        full_link = "https://habr.com" + link
        date = item.find('time')['title']
        news_items.append((title, full_link, date))

    return news_items


news_items = parse_website(os.getenv("url"))
for title, full_link, date in news_items:
    print(f"Заголовок: {title}")
    print(f"Ссылка: {full_link}")
    print(f"Дата: {date}")
    print("-" * 20)

def connect_to_database(db_config):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        return connection, cursor
    except mysql.connector.Error as err:
        print(f"Ошибка при подключении к базе данных: {err}")
        return None

def update_database():
    connection, cursor = connect_to_database(db_config)
    if connection:
        print("Успешное подключение к базе данных.")

        # Проверка наличия таблицы news
        cursor.execute("SHOW TABLES LIKE 'news'")
        table_exists = cursor.fetchone()

        if not table_exists:
            create_table_query = """
            CREATE TABLE news (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                link VARCHAR(255) NOT NULL,
                date DATETIME NOT NULL
            )
            """
            cursor.execute(create_table_query)
            print("Таблица news создана.")

        # Вставка данных в таблицу news
        news_items = parse_website(os.getenv("url"))
        for title, full_link, date in news_items:
            # Проверка, есть ли уже запись с таким заголовком и ссылкой
            cursor.execute(
                "SELECT id FROM news WHERE title = %s AND link = %s",
                (title, full_link)
            )
            existing_record = cursor.fetchone()

            if not existing_record:
                sql = "INSERT INTO news (title, link, date) VALUES (%s, %s, %s)"
                val = (title, full_link, date)
                cursor.execute(sql, val)
                connection.commit()
                print(f"Добавлено: {title}")
            else:
                print(f"Запись с заголовком '{title}' уже существует в базе.")

        # Проверка записей в таблице news
        cursor.execute("SELECT * FROM news")
        rows = cursor.fetchall()
        print(rows)

        cursor.close()
        connection.close()

update_database()
# Планирование выполнения функции каждые 6 часов
schedule.every(6).hours.do(update_database)

while True:
    schedule.run_pending()
    time.sleep(1)