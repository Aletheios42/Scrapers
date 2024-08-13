# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
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
        self.conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database="books",
        )

        # create cursor to execute commandans
        self.cur = self.conn.cursor()

    def create_table_if_not_exists(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS books (
            id int NOT NULL auto_increment,
            url VARCHAR(255) PRIMARY KEY,
            title text,
            upc VARCHAR(255),
            product_type VARCHAR(255),
            price_excl_tax DECIMAL,
            price_incl_tax DECIMAL,
            tax DECIMAL,
            price DECIAMAL,
            availability INT,
            num_reviews INT,
            star INT,
            category VARCHAR(255),
            description TEXT,
            PRIMARY KEY (id)
        );
        """

    def process_item(self, item, spider):

        # defien insert
        self.cur.execute("""  insert into books (
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
            description,
        ) values (
            
        )
