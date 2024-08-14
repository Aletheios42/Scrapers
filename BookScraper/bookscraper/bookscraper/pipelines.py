# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import mysql.connector
from mysql.connector import errorcode
from itemadapter import ItemAdapter


class BookscraperPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        # trim all white spaces
        field_names = adapter.field_names()
        for field_name in field_names:
            if field_name != "description":
                value = adapter.get(field_name)
                adapter[field_name] = value[0].strip()

        # category && product type ----> lowercase
        lowercase_keys = ["category", "product_type"]
        for lowercase_key in lowercase_keys:
            value = adapter.get(lowercase_key)
            adapter[lowercase_key] = value.lower()

        # Price  ----> to float
        price_keys = ["price", "price_excl_tax", "price_incl_tax", "tax"]
        for price_key in price_keys:
            value = adapter.get(price_key)
            value = value.replace("£", "")
            adapter[price_key] = float(value)

        # availability --> extract number of books in stock
        availability_string = adapter.get("availability")
        split_string_array = availability_string.split("(")
        if len(split_string_array) < 2:
            adapter["availability"] = 0
        else:
            availability_array = split_string_array[1].split(" ")
            adapter["availability"] = int(availability_array[0])

        # review into integer
        num_reviews_string = adapter.get("num_reviews")
        adapter["num_reviews"] = int(num_reviews_string)

        # star rating into integer
        # Diccionario para mapear palabras a números
        star_map = {"zero": 0, "one": 1, "two": 2,
                    "three": 3, "four": 4, "five": 5}
        star_string = adapter.get("star")
        split_star_array = star_string.split(" ")
        star_text_value = split_star_array[1].lower()
        adapter["star"] = star_map[star_text_value]

        return item


class SaveToMySQLPipeline:
    def __init__(self):
        try:
            self.conn = mysql.connector.connect(
                host="mysql",  # Cambia de 'localhost' a 'mysql' para Docker
                user="root",
                password="rootpassword",  # Usa la contraseña correcta
                database="books",  # Usa la base de datos 'books'
            )

            self.cur = self.conn.cursor()

            # Crear la tabla si no existe
            self.create_table_if_not_exists()
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
            raise

    def create_table_if_not_exists(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS books (
            id INT NOT NULL AUTO_INCREMENT,
            url VARCHAR(255) NOT NULL,
            title TEXT,
            upc VARCHAR(255),
            product_type VARCHAR(255),
            price_excl_tax DECIMAL(10, 2),
            price_incl_tax DECIMAL(10, 2),
            tax DECIMAL(10, 2),
            price DECIMAL(10, 2),
            availability INT,
            num_reviews INT,
            star INT,
            category VARCHAR(255),
            description TEXT,
            PRIMARY KEY (id),
            UNIQUE KEY (url)
        );
        """
        try:
            self.cur.execute(create_table_query)
            self.conn.commit()
        except mysql.connector.Error as err:
            print(f"Failed creating table: {err}")
            raise

    def process_item(self, item, spider):
        self.cur.execute(
            """
            INSERT INTO books (
                url,
                title,
                upc,
                product_type,
                price_excl_tax,
                price_incl_tax,
                tax,
                price,
                availability,
                num_reviews,
                star,
                category,
                description
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                item.get("url")[0] if item.get("url") else None,
                item.get("title")[0] if item.get("title") else None,
                item.get("upc")[0] if item.get("upc") else None,
                item.get("product_type")[0] if item.get(
                    "product_type") else None,
                float(item.get("price_excl_tax")[
                      0].replace("£", "").replace(",", ""))
                if item.get("price_excl_tax")
                else None,
                float(item.get("price_incl_tax")[
                      0].replace("£", "").replace(",", ""))
                if item.get("price_incl_tax")
                else None,
                float(item.get("tax")[0].replace("£", "").replace(",", ""))
                if item.get("tax")
                else None,
                float(item.get("price")[0].replace("£", "").replace(",", ""))
                if item.get("price")
                else None,
                int(item.get("availability")[0].split()[0])
                if item.get("availability")
                else None,
                int(item.get("num_reviews")[0]) if item.get(
                    "num_reviews") else None,
                int(item.get("star")[0]) if item.get("star") else None,
                item.get("category")[0] if item.get("category") else None,
                item.get("description")[0] if item.get(
                    "description") else None,
            ),
        )
        self.conn.commit()
        return item

    def close_spider(self, spider):
        self.cur.close()
        self.conn.close()
