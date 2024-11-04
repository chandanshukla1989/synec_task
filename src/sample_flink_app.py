from pyflink.datastream import StreamExecutionEnvironment

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    numbers = [1, 2, 3, 4, 5]

    number_stream = env.from_collection(collection=numbers)

    doubled_stream = number_stream.map(lambda x: x * 2)

    doubled_stream.print()

    env.execute("Simple Number Doubling Job")

if __name__ == "__main__":
    main()
