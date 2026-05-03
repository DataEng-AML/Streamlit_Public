# Example usage with your PipelineCrewAIPyDEAML
from test101 import PipelineCrewAIPyDEAML
from pipeline_metrics_agent import PipelineEvaluator
import pandas as pd

# Initialize evaluator and pipeline
evaluator = PipelineEvaluator()
pipeline = PipelineCrewAIPyDEAML()

# Create test cases
success_test_cases = []

# Test Case 1: Missing data handling
test_data_1 = pd.DataFrame({
    'customer name': ['Alice', None, 'Bob', None, 'Charlie'],
    'amount': [100.0, 200.0, None, 400.0, 500.0],
    'date': ['2023-01-01', None, '2023-01-03', '2023-01-04', None]
})

# Expected output (what your pipeline should produce)
expected_1 = test_data_1.copy()
expected_1['customer name'] = expected_1['customer name'].fillna('NoData')
expected_1['amount'] = expected_1['amount'].fillna(0)
expected_1['date'] = expected_1['date'].fillna('NoData')

success_test_cases.append(('missing_data_handling', test_data_1, expected_1))

# Test Case 2: Column standardization
test_data_2 = pd.DataFrame({
    'CustomerName': ['Alice', 'Bob'],
    'SalesAmount': [100, 200],
    'Transaction Date': ['2023-01-01', '2023-01-02']
})

expected_2 = pd.DataFrame({
    'customer_name': ['Alice', 'Bob'],
    'sales_amount': [100, 200],
    'transaction_date': ['2023-01-01', '2023-01-02']
})

success_test_cases.append(('column_standardization', test_data_2, expected_2))

# Error test cases (designed to potentially break the pipeline)
error_test_cases = [
    ('empty_dataframe', pd.DataFrame()),
    ('special_chars_columns', pd.DataFrame({'col@name': [1, 2], 'col#2': [3, 4]})),
    ('mixed_types', pd.DataFrame({'col': [1, 'text', 3.0, None]})),
]

# Run comprehensive evaluation
results = evaluator.comprehensive_evaluation(
    pipeline_func=pipeline.detect_missing_data,  # Adjust based on your entry point
    success_test_cases=success_test_cases,
    error_test_cases=error_test_cases,
    quality_test_data=(test_data_1, expected_1)  # Original vs processed for quality scoring
)

# Generate report
report = evaluator.generate_report(results, 'pipeline_evaluation_report.json')
print(report)