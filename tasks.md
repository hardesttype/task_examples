# Task 1 (SQL)

> Вывести топ 5 продавцов по продажам за последние 3 мес., среди тех продавцов, у которых текущие остатки на складах выше, чем их среднедневные продажи за последние 3 мес.

Таблица sales:
* `dt` - дата (день) отчета
* `seller_id` - идентификатор продавца
* `category_id` - идентификатор категории товара
* `sale_price` - сумма продаж

Таблица remainings:
* `dt` - дата (день) отчета
* `seller_id` - идентификатор продавца
* `category_id` - идентификатор категории товара
* `stock_id` - идентификатор склада
* `stock_price` - конечная стоимость остатков для покупателя

# Solution

```sql
with daily_sales_3m as (
  seller_id,
  avg(sum_sales) as avg_daily_sales,
  sum(sum_sales) as sum_sales
  from (
    select dt, seller_id, sum(sale_price) as sum_sales
	from sales
	where 1=1
	      and dt >= add_month(sysdate, -3)
	group by dt, seller_id
  )
),
current_remainings as (
    select dt, seller_id, sum(stock_price) as sum_stocks
	from sales
	where 1=1
	      and dt = trunc(sysdate)
	group by dt, seller_id
)
select *
from (
  select ds.seller_id,
	     ds.sum_sales,
		 row_number() over(partition_by ds.seller_id order by ds.sum_sales desc) as rn
  from daily_sales_3m     ds
  left 
  join current_remainings cr
        on cr.seller_id = ds.seller_id
       and ds.avg_daily_sales > nvl(cr.sum_stocks, 0)
) where rn <= 5
```

# Task 2 (Python)

> Даны файлы вида `number_str.ext`.
> Отсортировать массив по возростанию `number` и убыванию `str` в лексикографическом порядке.

```python
files = [
  '01_a.txt',
  '663_nn.txt',
  '1_bbb.txt',
  '009_casd.txt',
  '12_rte.txt',
]
```

# Solution

```python
def sort_key(file_name):
	x = -int(file_name.split('_')[0])
	y = file_name.split('_')[1].split('.')[0]
	return x, y
	
files = sorted(files, key = lambda x: sort_key(x), reverse = True)
```

# Task 3 (Python)

> Дан массив неповторяющихся положительных чисел `nums` длины N.
> Дана функция random(k: int) -> int, которая возвращает случайное целое число из множества {0, 1, 2, ..., k - 1}
> Необходимо напечатать все элементы из `nums` в случайном порядке, не повторяясь.

# Solution

```python
nums = [1, 2, 4, 7, 9]
N = lens(nums)
N_c = N
printed_idx = set()

while True:
    random_idx = random(N_c)
	if random_idx not printed_idx:
	    print(nums[random_idx])
		printed_idx.update(random_idx)
	
	nums[-1], nums[random_idx] = nums[random_idx], nums[-1] # Стоимость = O(1)
	
	N_c =- 1
	
	if len(printed_idx) == N:
	    break

# Общая сложность O(n)

```
