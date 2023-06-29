import random
import time
import ray

@ray.remote
def estimate_pi(num_points):
    points_inside_circle = 0
    points_total = 0

    for _ in range(num_points):
        x = random.uniform(0, 1)
        y = random.uniform(0, 1)
        distance = x**2 + y**2

        if distance <= 1:
            points_inside_circle += 1
        points_total += 1

    pi_estimate = 4 * points_inside_circle / points_total
    return pi_estimate

# 设置要生成的随机点数
num_points = 100000000

# 启动 Ray
ray.init()

# 开始计时
start_time = time.time()

# 提交任务给 Ray
pi_remote = estimate_pi.remote(num_points)

# 获取结果
pi = ray.get(pi_remote)

# 结束计时
end_time = time.time()

# 停止 Ray
ray.shutdown()

# 计算执行时间
execution_time = end_time - start_time

print(f"估算的π的值为: {pi}")
print(f"计算执行时间为: {execution_time}秒")
