#!/bin/bash
set +e  # Don't exit on errors
VIA_EXIT_CODE="${1:-0}"

echo "DEBUG: Current directory: $(pwd)"
echo "DEBUG: Looking for vi-analyzer-report.htm"
ls -la vi-analyzer-report.htm 2>&1 || echo "DEBUG: File not found in current directory"

if [ ! -f "vi-analyzer-report.htm" ]; then
    echo "No VI Analyzer report found"
    echo "DEBUG: Files in current directory:"
    ls -la
    exit $VIA_EXIT_CODE
fi

if [ ! -f "vi-analyzer-report.htm" ]; then
    echo "No VI Analyzer report found"
    exit $VIA_EXIT_CODE
fi

echo ""
echo "=================================================="
echo " VI Analyzer Results"
echo "=================================================="
echo ""

# Extract results
VIS_ANALYZED=$(grep -oP 'VIs Analyzed</td><td>\K[0-9]+' vi-analyzer-report.htm || echo "0")
TESTS_RUN=$(grep -oP 'Total Tests Run</td><td>\K[0-9]+' vi-analyzer-report.htm || echo "0")
PASSED=$(grep -oP 'Passed Tests</td><td>\K[0-9]+' vi-analyzer-report.htm || echo "0")
FAILED=$(grep -oP 'Failed Tests</td><td>\K[0-9]+' vi-analyzer-report.htm || echo "0")
SKIPPED=$(grep -oP 'Skipped Tests</td><td>\K[0-9]+' vi-analyzer-report.htm || echo "0")

# Extract error counts
VI_NOT_LOADABLE=$(grep -oP 'VI not loadable</td><td>\K[0-9]+' vi-analyzer-report.htm || echo "0")
TEST_NOT_LOADABLE=$(grep -oP 'Test not loadable</td><td>\K[0-9]+' vi-analyzer-report.htm || echo "0")
TEST_NOT_RUNNABLE=$(grep -oP 'Test not runnable</td><td>\K[0-9]+' vi-analyzer-report.htm || echo "0")
TEST_ERROR_OUT=$(grep -oP 'Test error out</td><td>\K[0-9]+' vi-analyzer-report.htm || echo "0")

echo "VIs Analyzed:   $VIS_ANALYZED"
echo "Total Tests:    $TESTS_RUN"
echo "Passed:         $PASSED"
echo "Failed:         $FAILED"
echo "Skipped:        $SKIPPED"
echo ""

if [ "$FAILED" -gt 0 ]; then
    echo "=================================================="
    echo " Failed Tests Summary"
    echo "=================================================="

    # Extract failed test details
    awk '
        /<a name="fail">Failed Tests/,/<a name="err">Testing Errors/ {
        if (match($0, /<b>([^<]+\.vi)<\/b>/, arr)) {
            if (vi_name) print ""
            vi_name = arr[1]
            printf "\n %s\n", vi_name
            print "---"
        }
        if (match($0, /<tr><td>([^<]+)<\/td><td>([^<]+)<\/td><\/tr>/, arr)) {
            test_name = arr[1]
            failure_msg = arr[2]
            printf " %s\n     → %s\n", test_name, failure_msg
        }
        }
    ' vi-analyzer-report.htm
    echo ""
    HAS_ERRORS=1
fi

if [ "$VI_NOT_LOADABLE" -gt 0 ]; then
    echo "=================================================="
    echo " VI Not Loadable Errors"
    echo "=================================================="

    awk '
        /<h3>VI Not Loadable<\/h3>/,/<\/table>/ {
            # Extract all table rows from the line
            line = $0
            while (match(line, /<tr><td>([^<]+)<\/td><td>([^<]+)<\/td><td>([^<]+)<\/td><\/tr>/, arr)) {
                vi_name = arr[1]
                vi_path = arr[2]
                error_msg = arr[3]
                printf "\n %s\n", vi_name
                printf "   Path: %s\n", vi_path
                printf "   → %s\n", error_msg
                # Remove the matched portion and continue
                line = substr(line, RSTART + RLENGTH)
            }
        }
    ' vi-analyzer-report.htm
    echo ""
    HAS_ERRORS=1
fi     

if [ "$TEST_NOT_LOADABLE" -gt 0 ]; then
    echo "=================================================="
    echo " Test Not Loadable Errors"
    echo "=================================================="

    awk '
        /<h3>Test Not Loadable<\/h3>/ { in_section = 1; next }
        in_section && /<h3>/ { exit }
        in_section && /<br><b>([^<]+)<\/b>/ {
            match($0, /<br><b>([^<]+)<\/b>/, arr)
            if (current_test != "") print ""
            current_test = arr[1]
            printf "\n[%s]\n", current_test
            next
        }
        in_section && /<table border=1>/ {
            # Extract all table rows from this line and subsequent lines
            line = $0
            # Keep reading lines until we hit </table> or next section
            while (line !~ /<\/table>/ && getline nextline > 0) {
                line = line nextline
                if (line ~ /<\/table>/) break
            }
            # Now process all rows in the accumulated line
            while (match(line, /<tr><td>([^<]+)<\/td><td>([^<]+)<\/td><td>([^<]+)<\/td><\/tr>/, arr)) {
                vi_name = arr[1]
                vi_path = arr[2]
                error_msg = arr[3]
                printf "  %s\n", vi_name
                printf "    → %s\n", error_msg
                line = substr(line, RSTART + RLENGTH)
            }
        }
    ' vi-analyzer-report.htm
    echo ""
    HAS_ERRORS=1
fi

if [ "$TEST_NOT_RUNNABLE" -gt 0 ]; then
    echo "=================================================="
    echo " Test Not Runnable Errors"
    echo "=================================================="

    awk '
        /<h3>Test Not Runnable<\/h3>/ { in_section = 1; next }
        in_section && /<h3>/ { exit }
        in_section && /<br><b>([^<]+)<\/b>/ {
            match($0, /<br><b>([^<]+)<\/b>/, arr)
            if (current_test != "") print ""
            current_test = arr[1]
            printf "\n[%s]\n", current_test
            next
        }
        in_section && /<table border=1>/ {
            # Extract all table rows from this line and subsequent lines
            line = $0
            # Keep reading lines until we hit </table> or next section
            while (line !~ /<\/table>/ && getline nextline > 0) {
                line = line nextline
                if (line ~ /<\/table>/) break
            }
            # Now process all rows in the accumulated line
            while (match(line, /<tr><td>([^<]+)<\/td><td>([^<]+)<\/td><td>([^<]+)<\/td><\/tr>/, arr)) {
                vi_name = arr[1]
                vi_path = arr[2]
                error_msg = arr[3]
                printf "  %s\n", vi_name
                printf "    → %s\n", error_msg
                line = substr(line, RSTART + RLENGTH)
            }
        }
    ' vi-analyzer-report.htm
    echo ""
    HAS_ERRORS=1
fi

if [ "$TEST_ERROR_OUT" -gt 0 ]; then
    echo "=================================================="
    echo " Test Error Out Errors"
    echo "=================================================="

    awk '
        /<h3>Test Error Out<\/h3>/ { in_section = 1; next }
        in_section && /<h3>/ { exit }
        in_section && /<br><b>([^<]+)<\/b>/ {
            match($0, /<br><b>([^<]+)<\/b>/, arr)
            if (current_test != "") print ""
            current_test = arr[1]
            printf "\n[%s]\n", current_test
            next
        }
        in_section && /<table border=1>/ {
            # Extract all table rows from this line and subsequent lines
            line = $0
            # Keep reading lines until we hit </table> or next section
            while (line !~ /<\/table>/ && getline nextline > 0) {
                line = line nextline
                if (line ~ /<\/table>/) break
            }
            # Now process all rows in the accumulated line
            while (match(line, /<tr><td>([^<]+)<\/td><td>([^<]+)<\/td><td>([^<]+)<\/td><\/tr>/, arr)) {
                vi_name = arr[1]
                vi_path = arr[2]
                error_msg = arr[3]
                printf "  %s\n", vi_name
                printf "    → %s\n", error_msg
                line = substr(line, RSTART + RLENGTH)
            }
        }
    ' vi-analyzer-report.htm
    echo ""
    HAS_ERRORS=1
fi

if [ "$HAS_ERRORS" -eq 1 ]; then
    echo ""
    echo "=================================================="
    echo "See full report in artifacts for complete details."
    echo "=================================================="
else
    echo " All tests passed!"
    echo "=================================================="
fi

exit $VIA_EXIT_CODE