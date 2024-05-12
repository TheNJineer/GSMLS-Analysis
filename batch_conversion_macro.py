import xlwings as xl
import os

previous_wd = os.getcwd()
file_path = 'C:\\Users\\Omar\\Desktop\\STF\\GSMLS'
macro_file = os.path.join(file_path, 'Batch Conversion Macro.xlsm')
os.chdir(file_path)

wb = xl.Book(macro_file)
macro = wb.macro('BatchConvertXLSToXLSX')
macro()


