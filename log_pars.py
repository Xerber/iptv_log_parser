import re
import pymysql
from datetime import datetime
import config


# Функция которая возвращает нынешнее время
def time():
  timestamps=str(datetime.now())
  timestamp = timestamps.split(".")
  return timestamp[0]
#--
#--Подключение к БД--
db = pymysql.connect(host=config.host, user=config.user,password=config.password,
                     db=config.db, charset='utf8mb4')
print (time(),"bd connected!")
cur = db.cursor() # формируем курсор для работы с sql-запросами
#--
#--РВ--
regexp = re.compile(r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})') # ip
regexp1 = re.compile('stream\?id') # слово stream
regexp2 = re.compile(r'\&') # знак &
#--
#--Открываем файл и записываем его в переменную file_content--
file = open('iptv.log.sample', 'r')
file_content = file.read()
file.close()
#--
#--Разбиваем файл на список, по строкам и проверяем не пустой ли он--
values = file_content.splitlines()
if len(values)==0:
  print("Файл пуст!")
#--
else:
  a = 0
  i = 0
  while (i<len(values)):
    value = values[a].split(" ")
    #Отсеиваем не подходящие строки по длине
    lenght = (len(value)>=8)
    if lenght:
      #Выбираем строчки которые подходят под наши РВ
      find = re.search(regexp, value[0]) # ip
      finds = re.search(regexp1, value[6]) # слово stream
      findss = re.search(regexp2, value[6]) # знак &
      if find and finds: # Если есть IP и stream
        #Редактируем вывод даты ( Убираем "[")
          s = value[3]
          s = s[1:len(s)]
          value[3] = s
          if findss: # Если есть & 
              #Редактируем вывод stream
              z = value[6].split("=") # ['/stream?id', '3661&token', '009b8053ee4703c58e715d4cabaec236']
              z = z[1].split("&") # ['3661', 'token']
              value[1] = z[0]
              value[2] = z[1]
              cur = db.cursor()
              sql = """INSERT INTO info(ip, date, stream, ans, size, view) VALUES
                    ('%(ip)s','%(date)s','%(stream)s','%(ans)s','%(size)s','%(view)s')
                    """%{"ip":value[0],"date":value[3],"stream":value[1],"ans":value[8],"size":value[9],"view":value[2]}
              cur.execute(sql) # Исполняем sql-запрос
              db.commit() # Применяем изменения к БД
              a += 1
              i += 1
              continue
          #Редактируем вывод id_stream
          z = value[6].split("=") # z = ['/stream?id', '3679']
          value[1] = z[1]
          cur = db.cursor()
          sql = """INSERT INTO info(ip, date, stream, ans, size) VALUES
                ('%(ip)s','%(date)s','%(stream)s','%(ans)s','%(size)s')
                """%{"ip":value[0],"date":value[3],"stream":value[1],"ans":value[8],"size":value[9]}
          cur.execute(sql) # Исполняем sql-запрос
          db.commit() # Применяем изменения к БД
          a += 1
          i += 1
      else:
          a += 1
          i += 1
    else:
      a += 1
      i += 1
db.close() # Закрываем соединение с БД
print(time(),'I\'m done, bd is close!')
#--Открываем файл и стираем его--
open('iptv.log.sample', 'w').close()
print(time(),'Файл очищен!')
#--
