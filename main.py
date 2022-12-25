from backend.zip_work import ZipWork


if __name__ == '__main__':
    work = ZipWork()
    work.create_data()
    print('ZIP files have been created')
    work.parse_zip_files()
    print('CSV files have been created')
