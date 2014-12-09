## This script will convert all problems in the 'problems' directory to plain text in the problems.text directory

ls problems |xargs -iXX echo 'echo problems/XX | iconv -f utf-8 -t ascii//translit | python html2text.py  > problems.text/XX'|bash
