FROM python:3.10

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Añadir wait-for-it script
COPY wait-for-it.sh /wait-for-it.sh
RUN chmod +x /wait-for-it.sh

COPY . .

# Espera a que MySQL esté disponible antes de ejecutar Scrapy
CMD ["./wait-for-it.sh", "mysql:3306", "--", "scrapy", "crawl", "bookspider"]

