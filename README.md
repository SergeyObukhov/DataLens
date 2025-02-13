# DataLens

## Статистика по ответам менеджеров в сделках

### Задача:
1. Написать SQL-запрос, который будет рассчитывать среднее время ответа для каждого сообщения.

    Расчёт должен учитывать следующее: 
    * если в диалоге идут несколько сообщений подряд от клиента или менеджера, то при расчёте времени ответа надо учитывать только первое сообщение из каждого блока; 
    * менеджеры работают с 09:30 до 00:00, поэтому нерабочее время не должно учитываться в расчёте среднего времени ответа, т.е. если клиент написал в 23:59, а менеджер ответил в 09:30 – время ответа равно одной минуте; 
    * ответы на сообщения, пришедшие ночью также нужно учитывать.
2. Построить дашборд в DataLens с данными о среднем времени ответа менеджеров.
3. Решить первое задание при помощи Python и библиотеки pandas.

#### Этапы работы:  
1. **Создание подлючения к базе данных Postgres.**
2. **Разработка SQL-запроса, для расчета времени ответа менеджера на каждое сообщение клиента.**
3. **Создание датасетов с логом сообщений, и средним времене ответов.**
4. **На основе датасетов создем чарты для визуализации KPI, и статистических данных.**
5. **Компановка чартов в дашборд.**
6. **Добавление селекторов, и настройка связей между ними и датасетами.**

### Результаты  
- [SQL-запрос](https://github.com/SergeyObukhov/DataLens/blob/master/sql_query.sql)
- [Дашборд](https://datalens.yandex/d7q90qeos4h6z)
- [Pandas-решение](https://github.com/SergeyObukhov/DataLens/blob/master/pandas_relation.ipynb)
