from opensearchpy import OpenSearch, helpers

client = OpenSearch(
    hosts=[{'host': "192.168.0.13", 'port': 9200}],
    http_compress=True,
    http_auth=("admin", "admin"),
    use_ssl=True,
    verify_certs=False,
    ssl_assert_hostname=False,
    ssl_show_warn=False
)


def demo2(string):
    return string.startswith("https://github.com/apache/cassandra.git")


def get_repo_list():
    results = client.search(index="git_raw_new", body={
        "size": 0,
        "aggs": {
            "group_by_origin": {
                "terms": {
                    "field": "origin",
                    "size": 50000
                }
            }
        }
    })

    print(len(results["aggregations"]["group_by_origin"]["buckets"]))
    list1 = []
    for sha in results["aggregations"]["group_by_origin"]["buckets"]:
        # print(sha["key"])
        list1.append(sha["key"])
    list2 = []
    for i in list1:
        if i.startswith("https://github.com/apache"):
            list2.append(i)
    print(len(list2))
    print(list2)
    return list2


def get_huawei_email1():
    # list1 = demo1()
    # print("******************************")
    # for origin in list1:
    #     print(origin)
    response = helpers.scan(client=client, index="git_raw_new", query={

        "query": {
            "bool": {
                "should": [
                    {"wildcard": {
                        "data.Author": {
                            "value": "*huawei.com*"
                        }
                    }}, {"wildcard": {
                        "data.Commit": {
                            "value": "*huawei.com*"
                        }
                    }}
                ]
            }
        }
        , "_source": ["origin", "data.Author", "data.Commit"]
    })
    list1 = []
    dict1 = {}
    for i in response:
        if i["_source"]["origin"][19:-4].startswith("apache"):
            print(i["_source"]["origin"][19:-4], i["_source"]["data"]["Commit"], i["_source"]["data"]["Author"])
            if i["_source"]["data"]["Commit"].find("huawei.com") != -1:
                list1.append(f'{i["_source"]["origin"][19:-4]}|{i["_source"]["data"]["Commit"]}')
            if i["_source"]["data"]["Author"].find("huawei.com") != -1:
                list1.append(f'{i["_source"]["origin"][19:-4]}|{i["_source"]["data"]["Author"]}')

        # if i["_source"]["data"]["Commit"].upper().find("HUAWEI"):
        #     print(i["_source"]["data"]["Commit"])
        # else:
    print(len(list1))
    list2 = list(set(list1))
    print(len(list2))
    print(list2)
    import csv
    # 1. 创建文件对象
    f = open('huaweiemail.csv', 'w+', encoding='utf-8', newline='')

    # 2. 基于文件对象构建 csv写入对象
    csv_writer = csv.writer(f, delimiter=',')
    for i in list2:
        list3 = i.split("|")
        csv_writer.writerow(
            [list3[0], list3[1]])


def get_huawei_email2():
    list1 = get_repo_list()
    print("******************************")
    list2 = []
    for origin in list1:
        print(origin)
        response = helpers.scan(client=client, index="git_raw_new", query={
            "query": {
                "term": {
                    "origin": {
                        "value": origin
                    }
                }
            }, "_source": ["origin", "data.Author", "data.Commit"]
        })
        for i in response:
            commit = i["_source"]["data"]["Commit"]
            author = i["_source"]["data"]["Author"]
            print(commit,author)
            if commit.find("huawei.com")!=-1:
                list2.append(commit)
            if author.find("huawei.com")!=-1:
                list2.append(author)
    print(len(list2))
    list3 = list(set(list2))
    print(len(list3))


get_huawei_email1()
