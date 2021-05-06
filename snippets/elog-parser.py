from typing import Dict, Any
import copy
from bs4 import BeautifulSoup
import pandas as pd


def ELOGParser(soup) -> Dict[int, Any]:
    data_cell = soup.find_all("td", recursive=True)
    message = {}

    message_definition = {
        "status": None,
        "time": None,
        "author": None,
        "tag": None,
        "title": None,
        "content": None,
        "type": None,
    }
    header_counter = 0
    last_index = 0

    for td in data_cell:

        if "class" in td.attrs:

            # header
            if td.attrs["class"][0] == "list1":
                index = int(td.a.attrs["href"].split()[0].split("/")[-1])

                if index != last_index:
                    last_index = index

                    message[index] = copy.deepcopy(message_definition)
                    header_counter = 0

                message[index][
                    list(message_definition.keys())[header_counter]
                ] = td.a.get_text(strip=True)
                header_counter += 1

            # content
            elif td.attrs["class"][0] == "messagelist":
                for ei in td.find_all():

                    if ei.name == "table":
                        message[index]["type"] = ei.name
                        message[index]["content"] = pd.read_html(ei.prettify())

    return message


def pprint(index: int, message: Dict[str, Any]):
    print("Message {} ({})".format(index, message["time"]))
    print("Author: {}".format(message["author"]))
    print("Tag: {}".format(message["tag"]))
    print("Title: {}".format(message["title"]))
    print(message["content"], "\n")


if __name__ == "__main__":
    from tap import Tap

    class Parser(Tap):
        html: str  # File containing the html source (view-source:https://in.xfel.eu/elog/SPB-SFX+Proposal+2696/page?mode=full)
        entry: str = "all"  # Message index or 'all' to get an overview
        run: int = 0  # Column containg the run index
        run_index: int = 0  # Extract this run
        comment: int = 1  # Column containing the comment
        """Parse the ELOG"""

    args = Parser().parse_args()

    try:
        with open(args.html, "rb") as fh:
            soup = BeautifulSoup(fh, "html.parser")

    except FileNotFoundError:
        raise FileNotFoundError("{} is not accessible.".format(args.html))

    message = ELOGParser(soup)

    if args.entry == "all":
        for ki, vi in message.items():
            if vi["type"] == "table":
                pprint(ki, vi)

    else:
        table = message[int(args.entry)]

        run = table["content"][0][args.run]
        comment = table["content"][0][args.comment]

        index = -1
        for index in range(1, run.size):

            if pd.isna(run[index]):
                continue

            # run can be expressed as range
            if run[index].find("-") > 0:
                if int(args.run_index) in range(
                    *[int(ri) for ri in run[index].split("-")]
                ):
                    break
            else:
                if int(args.run_index) == int(run[index]):
                    break

        print("Found run {}\n{}".format(args.run_index, comment[index]))
