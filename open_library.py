import requests
import concurrent.futures
from pyspark.sql import functions as f
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('open-library').getOrCreate()


def get_records(iter_number):
    flattened_records = []
    re = requests.get(f'http://openlibrary.org/subjects/romance.json?offset={iter_number * 999}&limit=999')
    if re:
        records = re.json().get('works')
        for record in records:
            for author in record['authors']:
                flattened_records.append({'book_key': record['key'],
                                          'book_title': record['title'],
                                          'author_key': author['key'],
                                          'author_name': author['name'],
                                          'first_publish_year': record['first_publish_year']})
    return flattened_records


def run_multi_thread(iter_number):
    records = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = []
        for i in [i for i in range(0, iter_number)]:
            future = executor.submit(get_records, i)
            futures.append(future)
        for future in concurrent.futures.as_completed(futures):
            record = future.result()
            records.extend(record)
    return spark.createDataFrame(records).dropDuplicates()


def get_books(records):
    return records.select('book_key', 'book_title', 'first_publish_year').dropDuplicates()


def get_authors(records):
    return records.select('author_key', 'author_name').dropDuplicates()


def save_df_as_csv(df, file_name):
    df.coalesce(1).write.mode('overwrite').csv(file_name)


def data_analysis(records):
    records.createOrReplaceTempView("records")
    spark.sql("""select author_key, author_name, first_publish_year, count(book_key) as book_numbers
                 from records
                 group by author_key, author_name, first_publish_year
                 """).show()
    spark.sql("""with a as(select author_key, author_name, first_publish_year, count(book_key) as book_numbers
                           from records
                           group by author_key, author_name, first_publish_year)
                 select author_key, author_name, round(avg(book_numbers),2) as avg_book_number_per_year
                 from a group by author_key, author_name;""").show()


def write_into_db(df):
    df.write.format('jdbc').options(
        url='jdbc:mysql://localhost/database_name',
        driver='com.mysql.jdbc.Driver',
        dbtable='DestinationTableName',
        user='your_user_name',
        password='your_password').mode('append').save()


def main():
    work_count = requests.get('http://openlibrary.org/subjects/romance.json?').json().get('work_count')
    iter_number = work_count // 999

    # df
    records = run_multi_thread(iter_number)
    books = get_books(records)
    authors = get_authors(records)

    # save df
    save_df_as_csv(books, file_name='books.csv')
    save_df_as_csv(authors, file_name='authors.csv')

    # analysis
    data_analysis(records)


if __name__ == "__main__":
    main()
