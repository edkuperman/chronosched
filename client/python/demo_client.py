from chronosched_client import ChronoschedClient


def main():
    client = ChronoschedClient()
    print(client.healthz())


if __name__ == "__main__":
    main()
