import os
import datetime
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import NoSuchElementException

# Initialiser le driver
driver = webdriver.Chrome()
url = 'https://dl.acm.org/action/doSearch?AllField=Devops&ContentItemType=research-article&startPage=22&pageSize=20'
driver.get(url)

# Charger les données existantes à partir du fichier JSON
def load_existing_data(filename):
    if os.path.exists(filename):
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)
    return []

# Fermer la boîte de dialogue des cookies
def close_cookie_dialog():
    try:
        cookie_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, 'CybotCookiebotDialogBodyLevelButtonLevelOptinAllowallSelection'))
        )
        cookie_button.click()
    except Exception as e:
        print(f"Cookie dialog not found or already closed: {e}")

# Récupérer les articles sur la page
def get_articles():
    return driver.find_elements(By.CSS_SELECTOR, ".issue-item.issue-item--search.clearfix")

# Charger les données existantes
filename = 'DevOpsi.json'
existing_data = load_existing_data(filename)
existing_dois = {article['doi'] for article in existing_data}

article_data = []

close_cookie_dialog()

articles = get_articles()
print("Number of articles found:", len(articles))

topic = 'DevOps'
website = 'ACM'

# Parcourir les articles et collecter les données
for i in range(len(articles)):
    article = articles[i]
    try:
        type = driver.find_element(By.CLASS_NAME, 'issue-heading').text
    except NoSuchElementException:
        type = "Unknown"
    
    try:
        doi = article.find_element(By.CSS_SELECTOR, '.issue-item__detail a')
        doi_link = doi.get_attribute("href")
    except NoSuchElementException:
        print(f"Skipping article as DOI not found.")
        continue

    # Vérifier si l'article est déjà présent
    if doi_link in existing_dois:
        print(f"Skipping article with DOI {doi_link}, already exists in the dataset.")
        continue

    publisher_name = doi.get_attribute("title")
    try:
        metric = article.find_element(By.CLASS_NAME, 'metric').text
    except NoSuchElementException:
        metric = 'N/A'
    try:
        citation = article.find_element(By.CLASS_NAME, 'citation').text
    except NoSuchElementException:
        citation = 'N/A'

    title_main = article.find_element(By.CLASS_NAME, 'hlFld-Title').text
    try:
        title_sub = article.find_element(By.CLASS_NAME, 'hlFld-SubTitle').text
    except NoSuchElementException:
        title_sub = ''
    full_title = title_main + ' ' + title_sub

    article.find_element(By.CLASS_NAME, 'hlFld-Title').click()
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, 'abstract'))
    )
    abstract = driver.find_element(By.CSS_SELECTOR, 'section#abstract div[role="paragraph"]').text

    author_elements = WebDriverWait(driver, 10).until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR, '.authors .dropBlock a')))
    authors = [author.get_attribute("title").strip() for author in author_elements if author.get_attribute("title").strip()] or ["Unknown"]

    try:
        keywords_section = driver.find_element(By.CSS_SELECTOR, "section[property='keywords']")
        keywords_elements = keywords_section.find_elements(By.CSS_SELECTOR, "ol li a")
        keywords = [keyword.get_attribute("textContent").strip() for keyword in keywords_elements]
    except NoSuchElementException:
        keywords = ["N/A"]

    try:
        ISSN_container = driver.find_element(By.CSS_SELECTOR, "span.space")
        ISSN = ISSN_container.get_attribute("textContent").strip()
    except NoSuchElementException:
        ISSN = "N/A"

    date = driver.find_element(By.CLASS_NAME, 'core-date-published').text
    try:
        date_obj = datetime.datetime.strptime(date, '%d %B %Y')
    except ValueError:
        date_obj = datetime.datetime.now()

    try:
        authors_link = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'a.to-authors-affiliations'))
        )
        authors_link.click()

        author_sections = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'div.authInfo'))
        )

        universities = []
        countries = []
        authors_with_affiliations = []

        for section in author_sections:
            author_name = section.find_element(By.CSS_SELECTOR, 'span.auth-name').text.strip()
            affiliation_text = section.find_element(By.CSS_SELECTOR, 'span.auth-institution').text.strip()

            if ',' in affiliation_text:
                university, country = map(str.strip, affiliation_text.rsplit(',', 1))
            else:
                university, country = affiliation_text, "Unknown"

            universities.append(university)
            countries.append(country)

            authors_with_affiliations.append({
                "author": author_name,
                "university": university,
                "country": country,
                "location": affiliation_text
            })
    except Exception as e:
        universities = ["Unknown"]
        countries = ["Unknown"]
        authors_with_affiliations = []
        print(f"Error extracting affiliations: {e}")

    article_data.append({
        'title': full_title.strip(),
        'authors': authors,
        'authors_with_affiliations': authors_with_affiliations,
        'Date': date_obj.strftime('%d %B %Y'),
        'Year': int(date_obj.strftime('%Y')),
        'Month': date_obj.strftime('%B'),
        'Day': int(date_obj.strftime('%d')),
        'abstract': abstract,
        'doi': doi_link,
        'Downloads': int(metric.replace(',', '')) if metric != 'N/A' else 0,
        'citations': int(citation) if citation != 'N/A' else 0,
        'type': type,
        'publisher': {
            "name": publisher_name,
            "ISSN": ISSN
        },
        'universities': universities,
        'countries': countries,
        'keywords': keywords,
        'topic': topic,
        'website': website
    })

    print(f"Added new article with DOI {doi_link}")

    driver.back()
    WebDriverWait(driver, 10).until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".issue-item.issue-item--search.clearfix"))
    )
    articles = get_articles()

# Fusionner les nouvelles données avec les anciennes
existing_data.extend(article_data)

# Éliminer les doublons en utilisant le DOI comme identifiant unique
unique_data = {article['doi']: article for article in existing_data}.values()

# Enregistrer les données fusionnées dans le fichier JSON
with open(filename, 'w', encoding='utf-8') as f:
    json.dump(list(unique_data), f, ensure_ascii=False, indent=4)

print(f"Data saved to {filename}. Total articles: {len(unique_data)}")

driver.quit()
