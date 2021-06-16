from io import StringIO
from logging import log
from flask import Flask
import pandas as pd
import boto3
import re


s3 = boto3.resource('s3')
bucket = s3.Bucket('developyn-challenges')

def create_app():
  app = Flask(__name__)
  app.config['DEBUG'] = True

  @app.route('/meta')
  def meta():
    files = get_files()
    sorted_files = filter_and_sort_files(files, True)
    file_meta = []
    for file in sorted_files:
        file_meta.append({ "key": file.key, "size": f"{format(file.size / 1024, '.2f')} KB", "last_modified": f"{file.last_modified}" })
        print(f"File with key {file.key} is {format(file.size / 1024, '.2f')} KB and was last modified on {file.last_modified}")
    return ''.join(str(e) for e in file_meta)

  @app.route('/file/', defaults={'file_name': None})
  @app.route('/file/<file_name>')
  def file_info(file_name):
    file = None
    if file_name is None:
      invalid = True
      files = get_and_sort_files(reverse = True)
      num_of_files = len(list(files))
      index = 0
      regex = re.compile('csv$')

      while invalid and index < num_of_files:
        file = bucket.Object(files[index].key)
        if regex.search(file.key) is not None:
          invalid = False
        index += 1

    else:
      file = bucket.Object(f"python/{file_name}")

    file = file.get()
    body = file['Body']
    csv_string = body.read().decode('utf-8')

    df = pd.read_csv(StringIO(csv_string), sep=';')

    df = df.sort_values(by=['fixed acidity', 'quality'], ascending=[True, False])

    columns = list(df.columns)
    columns.remove('fixed acidity')
    columns.remove('quality')

    df = convert_to_descending_rank(df, columns);

    print(df)
    return df.to_html()


  return app



def get_files(prefix=''):
  print(prefix)
  return bucket.objects.filter(Prefix=f"python/{prefix}")

def extract_files(files):
    def extraction(file):
      regex = re.compile('/$')
      if regex.search(file.key) is None:
          return True
      else:
          return False
    
    return list(filter(extraction, files))

def sort_by_time(files, reverse = False):
    def sortKey(e):
      return e.last_modified
    
    return sorted(files, key = sortKey, reverse = reverse)

def filter_and_sort_files(files, reverse = False):
  filtered = extract_files(files);
  return sort_by_time(filtered, reverse = reverse)

def get_and_sort_files(reverse = False):
  files = get_files()
  return filter_and_sort_files(files, reverse = reverse)

def convert_to_descending_rank(df, columns):
  for col in columns:
    df[col] = df[col].rank(ascending = False)
  
  return df
