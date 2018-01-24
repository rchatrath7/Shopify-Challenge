import json
import sys

def merge_dict_excluding_key(left_object, right_object, left_key, right_key):
	joined = dict(left_object, **right_object)
	joined.pop(left_key, None)
	joined.pop(right_key, None)

	return joined

def inner_join(left_table, right_table, left_key, right_key):
	'''
	Takes two tables and returns an object containing after an inner join:
	* Result Count - Total number of rows in joined result
	* Rows Skipped on Left - Total number of rows in Left table that did not join
	* Rows Skipped on Right - Total number of rows Right table that did not join
	* Result - Array of joined objects
	'''
	output_table = {
		'result_count': 0,
		'rows_skipped_left': 0,
		'rows_skipped_right': 0,
		'result': [],
	}
	# Go through each row on the left table and check if there is a matching row on
	# The right table. If there is, add the merged rows to the results and add the right
	# row to a list of rows that are added (used later)
	# If it isn't, increment the rows_skipped_left field
	# We do the increments in case of having the same row from either left or right table
	# Appear more than once
	for left_row in left_table:
		left_row_skipped = True
		for right_row in right_table:
			if left_row[left_key] == right_row[right_key]:
				left_row_skipped = False
				right_rows.append(right_row)
				joined = merge_dict_excluding_key(left_row, right_row, left_key, right_key)
				output_table['result'].append(joined)

		if left_row_skipped:
			output_table['rows_skipped_left'] = output_table['rows_skipped_left'] + 1

	output_table['result_count'] = len(output_table['result'])
	# We can set the right_rows_skipped to the difference between the right table and
	# the number of rows added because the number of rows added is equal to the number
	# of rows from the right_table we chose
	output_table['rows_skipped_right'] = len(right_table) - output_table['result_count']

	return output_table

def outer_join(left_table, right_table, left_key, right_key):
	'''
	Takes two tables and returns an object containing after a left join:
	* Result Count - Total number of rows in joined result
	* Rows Skipped on Left - Total number of rows in Left table that did not join
	* Rows Skipped on Right - Total number of rows Right table that did not join
	* Result - Array of joined objects
	'''
	output_table = {
		'result_count': 0,
		'rows_skipped_left': 0,
		'rows_skipped_right': 0,
		'result': [],
	}

	for left_row in left_table:
		row_matched = False
		for right_row in right_table:
			if left_row[left_key] == right_row[right_key]:
				row_matched = True
				output_table['result_count'] = output_table['result_count'] + 1
				joined = merge_dict_excluding_key(left_row, right_row, left_key, right_key)
				output_table['result'] = output_table['result'].append(joined)

		if not row_matched:
			output_table['result_count'] = output_table['result_count'] + 1
			default_row = {k: None for k in right_table[0]}
			joined = merge_dict_excluding_key(left_row, default_row, left_key, right_key)
			output_table['result'] = output_table['result'].append(joined)

if __name__ == "__main__":
	if len(sys.argv) != 4 or sys.argv[1] not in ["inner", "left", "outer"]:
		print "Command should be in the format: python joined.py inner|left|outer file1.json file2.json"
		sys.exit(1)
	else:
		with open(sys.argv[2], 'r') as left:
			left_table = json.load(left)

		with open(sys.argv[3], 'r') as right:
			right_table = json.load(right)

		left_table_keys = left_table[0].keys()
		right_table_keys = right_table[0].keys()

		print "Please select the key to choose on the left table: "

		for i in xrange(len(left_table_keys)):
			print "[{0}] - {1}".format(i, left_table_keys[i])

		left_key_index = int(raw_input())
		if not (0 <= left_key_index < len(left_table_keys)):
			left_key_index = raw_input("Please select a number between 0 and {0}\n".format(len(left_table_keys)))

		left_key = left_table_keys[left_key_index]

		print "Please select the key to choose on the right table: "

		for i in xrange(len(right_table_keys)):
			print "[{0}] - {1}".format(i, right_table_keys[i])

		right_key_index = int(raw_input())
		if not 0 <= right_key_index < len(right_table_keys):
			right_key_index = raw_input("Please select a number between 0 and {0}\n".format(len(right_table_keys)))

		right_key = right_table_keys[right_key_index]

		if sys.argv[1] == "inner":
			print inner_join(left_table, right_table, left_key, right_key)
		elif sys.argv[1] == "left":
			print outer(left_table, right_table, left_key, right_key)
		else:
			print outer(right_table, left_table, right_key, left_key)
