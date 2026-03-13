import json
import random
import time

users = [f'user_{i}' for i in range(1, 1001)]
products = [f'product_{i}' for i in range(1, 501)]
total_records = 1000000
batch_size = 10000

start_time = time.time()
with open('user_logs_large.json', 'w') as f:
    for i in range(total_records):
        log = {
            "timestamp": int(time.time()) - random.randint(0, 90*24*3600),
            "user_id": random.choice(users),
            "action": random.choice(['view', 'click', 'purchase', 'add_to_cart']),
            "product_id": random.choice(products),
            "price": round(random.uniform(10, 1000), 2)
        }
        f.write(json.dumps(log) + '\n')
        if (i+1) % batch_size == 0:
            print(f"已生成 {i+1} 条日志...")

end_time = time.time()
print(f"完成！生成 {total_records} 条日志，耗时 {end_time - start_time:.2f} 秒")
