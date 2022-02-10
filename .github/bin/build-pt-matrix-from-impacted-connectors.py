#!/usr/bin/env python3

import argparse
import collections
import itertools
import yaml
import json
import logging
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(
        description="Filter test matrix modules using list of impacted modules."
    )
    parser.add_argument(
        "-m",
        "--matrix",
        type=argparse.FileType("r"),
        default=".github/test-pt-matrix.yaml",
        help="A YAML file with the PT matrix",
    )
    parser.add_argument(
        "-i",
        "--impacted",
        type=argparse.FileType("r"),
        default="impacted-connectors.log",
        help="List of impacted connectors, one per line",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=argparse.FileType("w"),
        default=sys.stdout,
        help="Filename to write JSON output to",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_const",
        dest="loglevel",
        const=logging.DEBUG,
        default=logging.INFO,
        help="Print info level logs",
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=args.loglevel, format="%(asctime)s %(levelname)s %(message)s"
    )
    build(args.matrix, args.impacted, args.output)


def excluded(item, excludes):
    result = any(exclude.items() <= item.items() for exclude in excludes)
    logging.debug("excluded(%s, %s) returns %s", item, excludes, result)
    return result


def expand_matrix(matrix):
    include = matrix.pop("include", [])
    exclude = matrix.pop("exclude", [])

    # for every key in the matrix dict, convert its values to tuples of key and value
    tuples = [[(k, v) for v in vals] for k, vals in matrix.items()]
    logging.debug("tuples: %s", tuples)
    # then calculate the product of such lists of tuples
    # and convert tuples back to a dict stored in a list
    combinations = list(map(dict, itertools.product(*tuples)))
    logging.debug("combinations: %s", combinations)
    # filter out excludes and add includes as the last step
    # so that excluded combinations can be explicitly added back
    return [item for item in combinations if not excluded(item, exclude)] + include


def load_available_connectors_for_config(config, suites):
    cmd = ['testing/bin/ptl', 'suite', 'describe', '--suite', ",".join(suites), '--config', "config-" + config, '--format', 'JSON']
    logging.debug("executing: %s", " ".join(cmd))
    process = subprocess.run(cmd,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE,
                             universal_newlines=True)
    logging.debug("ptl suite describe: %s", process)
    if process.returncode != 0:
        logging.error("ptl suite describe failed: %s", process)
        return {}
    for line in process.stdout.splitlines():
        if line.startswith('{'):
            logging.debug("Parsing JSON: %s", line)
            ptl_output = json.loads(line)
            logging.debug("Handling JSON object: %s", ptl_output)
            config_connectors = {(config, suite.get("name")): [connector for testRun in suite.get("testRuns", []) for connector in testRun["environment"].get("connectors", [])]
                                 for suite in ptl_output.get("suites", [])}
            logging.debug("config_connectors: %s", config_connectors)
            return config_connectors
    logging.error("ptl suite describe hasn't returned any JSON line: %s", process)
    return {}


def load_available_connectors(configToSuiteMap):
    available_connectors = {}
    for config, suites in configToSuiteMap.items():
        available_connectors.update(
            load_available_connectors_for_config(config, suites)
        )
    return available_connectors


def tested_connectors(available_connectors, config, suite):
    return available_connectors.get((config, suite), [])


def build(matrix_file, impacted_file, output_file):
    matrix = yaml.load(matrix_file, Loader=yaml.Loader)
    impacted = list(filter(None, [line.rstrip() for line in impacted_file.readlines()]))
    logging.info("Read matrix: %s", matrix)
    logging.info("Read impacted: %s", impacted)
    items = expand_matrix(matrix)
    logging.info("Expanded matrix: %s", items)

    configToSuiteMap = collections.defaultdict(list)
    for item in items:
        configToSuiteMap[item.get("config")].append(item.get("suite"))
    available_connectors = load_available_connectors(configToSuiteMap)
    if len(available_connectors) == 0:
        result = items
    else:
        result = []
        for item in items:
            connectors = tested_connectors(
                available_connectors, item.get("config"), item.get("suite")
            )
            logging.debug("impacted: %s, connectors: %s", impacted, connectors)
            if not any(connector in impacted for connector in connectors):
                logging.info("Excluding matrix entry due to connectors: %s", item)
                continue
            logging.info("Adding matrix entry: %s", item)
            result.append(item)
    json.dump({"include": result}, output_file)
    output_file.write("\n")


if __name__ == "__main__":
    main()
