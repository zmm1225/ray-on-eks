import random
import time
import ray

@ray.remote
def estimate_pi_part(num_points):
    points_inside_circle = 0
    points_total = 0

    for _ in range(num_points):
        x = random.uniform(0, 1)
        y = random.uniform(0, 1)
        distance = x**2 + y**2

        if distance <= 1:
            points_inside_circle += 1
        points_total += 1

    return points_inside_circle, points_total

def estimate_pi_parallel(num_points, num_workers):
    # 启动 Ray
    ray.init()

    # 提交任务给 Ray
    tasks = [estimate_pi_part.remote(num_points // num_workers) for _ in range(num_workers)]

    # 获取结果
    results = ray.get(tasks)

    # 结束 Ray
    ray.shutdown()

    # 统计结果
    total_inside_circle = sum([result[0] for result in results])
    total_points = sum([result[1] for result in results])

    # 计算 π 的估算值
    pi_estimate = 4 * total_inside_circle / total_points

    return pi_estimate

def main():
    # 设置要生成的随机点数和并行计算的工作进程数
    num_points = 100000000
    num_workers = 4

    # 开始计时
    start_time = time.time()

    # 执行并行计算
    pi = estimate_pi_parallel(num_points, num_workers)

    # 结束计时
    end_time = time.time()

    # 计算执行时间
    execution_time = end_time - start_time

    print(f"估算的π的值为: {pi}")
    print(f"计算执行时间为: {execution_time}秒")

if __name__ == "__main__":
    main()
