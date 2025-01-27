# splitted_metrics.py

"""
splitted_metrics.py
Collect BFS data for each interval: merges, closedIss, closedPR, forks, watchers, etc.
Ensure placeholders and parameters line up.
"""

from datetime import datetime

def gather_bfs_data(repo, intervals, cursor, debug_lines):
    """
    gather_bfs_data: For each (start_dt, end_dt) in intervals, query various BFS metrics
    and build a row dict: {
       'start_dt':..., 'end_dt':..., 'mergesRaw':..., 'closedIssRaw':..., ...
    }
    Return a list of these row dicts, one per interval in 'intervals'.
    If something goes wrong, raise or return empty.
    """
    results = []
    for (c_start, c_end) in intervals:
        row_data = {
            'start_dt': c_start,
            'end_dt': c_end,
            'partialCoverage': False,  # you might set True if c_end> now, etc.
            'mergesRaw': 0,
            'closedIssRaw': 0,
            'closedPRRaw': 0,
            'forksRaw': 0,
            'starsRaw': 0,
            'watchersRaw': 0,
            'commentsIssueRaw': 0,
            'commentsPRRaw': 0,
            'reactIssueRaw': 0,
            'reactPRRaw': 0,
            'distinctPartRaw': 0,
        }

        try:
            # 1) merges
            q_merges = """
              SELECT COUNT(*) 
              FROM pulls
              WHERE repo_name=%s
                AND created_at >= %s
                AND created_at < %s
                AND merged_at IS NOT NULL
            """
            # Here we have exactly 3 placeholders => pass exactly 3 params
            debug_lines.append(
              f"[SQL-Query merges for {repo} from {c_start} to {c_end}]\n"
              + q_merges.replace('\n',' ') + "\n"
              + f"[params] => (repo={repo}, c_start={c_start}, c_end={c_end})"
            )
            cursor.execute(q_merges, (repo, c_start, c_end))
            merges_count = cursor.fetchone()[0]
            row_data['mergesRaw'] = merges_count

            # 2) closed issues
            q_closedIss = """
              SELECT COUNT(*)
              FROM issues
              WHERE repo_name=%s
                AND created_at >= %s
                AND created_at < %s
                AND state='closed'
            """
            # again 3 placeholders => 3 parameters
            debug_lines.append(
              f"[SQL-Query closedIss for {repo} from {c_start} to {c_end}]\n"
              + q_closedIss.replace('\n',' ') + "\n"
              + f"[params] => (repo={repo}, c_start={c_start}, c_end={c_end})"
            )
            cursor.execute(q_closedIss, (repo, c_start, c_end))
            closed_iss_count = cursor.fetchone()[0]
            row_data['closedIssRaw'] = closed_iss_count

            # 3) closed PR
            q_closedPR = """
              SELECT COUNT(*)
              FROM pulls
              WHERE repo_name=%s
                AND created_at >= %s
                AND created_at < %s
                AND merged_at IS NULL
                AND state='closed'
            """
            # if you do that approach or store 'state' in DB, etc.
            debug_lines.append(
              f"[SQL-Query closedPR for {repo} from {c_start} to {c_end}]\n"
              + q_closedPR.replace('\n',' ') + "\n"
              + f"[params] => (repo={repo}, c_start={c_start}, c_end={c_end})"
            )
            cursor.execute(q_closedPR, (repo, c_start, c_end))
            closed_pr_count = cursor.fetchone()[0]
            row_data['closedPRRaw'] = closed_pr_count

            # 4) forks
            q_forks = """
              SELECT COUNT(*)
              FROM forks
              WHERE repo_name=%s
                AND created_at >= %s
                AND created_at < %s
            """
            debug_lines.append(
              f"[SQL-Query forks for {repo} from {c_start} to {c_end}]\n"
              + q_forks.replace('\n',' ') + "\n"
              + f"[params] => (repo={repo}, c_start={c_start}, c_end={c_end})"
            )
            cursor.execute(q_forks,(repo, c_start, c_end))
            row_data['forksRaw'] = cursor.fetchone()[0]

            # 5) watchers or watchersRaw?
            # watchers is tricky because no created_at column. You might just do a snapshot count.
            # If your watchers table doesn't store 'created_at', you can't do a time-based subset
            # e.g. watchers just do total watchers
            q_watchers = """
              SELECT COUNT(*)
              FROM watchers
              WHERE repo_name=%s
            """
            # only 1 placeholder => pass 1 param
            debug_lines.append(
              f"[SQL-Query watchers for {repo} (no times)]\n"
              + q_watchers.replace('\n',' ') + "\n"
              + f"[params] => (repo={repo})"
            )
            cursor.execute(q_watchers,(repo,))
            row_data['watchersRaw'] = cursor.fetchone()[0]

            # 6) stars
            q_stars = """
              SELECT COUNT(*)
              FROM stars
              WHERE repo_name=%s
                AND starred_at >= %s
                AND starred_at < %s
            """
            # 3 placeholders => pass 3 params
            debug_lines.append(
              f"[SQL-Query stars for {repo} from {c_start} to {c_end}]\n"
              + q_stars.replace('\n',' ') + "\n"
              + f"[params] => (repo={repo}, c_start={c_start}, c_end={c_end})"
            )
            cursor.execute(q_stars,(repo, c_start, c_end))
            row_data['starsRaw'] = cursor.fetchone()[0]

            # 7) commentsIssueRaw vs. commentsPRRaw
            # For example, we do a naive approach: comments in issue_comments table joined to issues or pulls?
            q_comm_issue = """
              SELECT COUNT(*)
              FROM issue_comments c
              JOIN issues i ON i.repo_name=c.repo_name AND i.issue_number=c.issue_number
              WHERE c.repo_name=%s
                AND c.created_at >= %s
                AND c.created_at < %s
                -- Additional logic to confirm it's an 'issue' not a 'pull' if you store that in DB
            """
            debug_lines.append(
              f"[SQL-Query commentsIssue for {repo} from {c_start} to {c_end}]\n"
              + q_comm_issue.replace('\n',' ') + "\n"
              + f"[params] => (repo={repo}, c_start={c_start}, c_end={c_end})"
            )
            cursor.execute(q_comm_issue,(repo, c_start, c_end))
            row_data['commentsIssueRaw'] = cursor.fetchone()[0]

            # 8) reactions on issue comments
            # e.g. you might define your own logic
            q_react_issue = """
              SELECT COUNT(*)
              FROM comment_reactions r
              WHERE r.repo_name=%s
                AND r.created_at >= %s
                AND r.created_at < %s
            """
            debug_lines.append(
              f"[SQL-Query reactIssue for {repo} from {c_start} to {c_end}]\n"
              + q_react_issue.replace('\n',' ') + "\n"
              + f"[params] => (repo={repo}, c_start={c_start}, c_end={c_end})"
            )
            cursor.execute(q_react_issue,(repo, c_start, c_end))
            row_data['reactIssueRaw'] = cursor.fetchone()[0]

            # etc. you can replicate for PR-based stuff, or skip if needed
            q_comm_pr = """
              SELECT COUNT(*)
              FROM issue_comments c
              JOIN pulls p ON p.repo_name=c.repo_name AND p.pull_number=c.issue_number
              WHERE c.repo_name=%s
                AND c.created_at >= %s
                AND c.created_at < %s
            """
            debug_lines.append(
              f"[SQL-Query commentsPR for {repo} from {c_start} to {c_end}]\n"
              + q_comm_pr.replace('\n',' ') + "\n"
              + f"[params] => (repo={repo}, c_start={c_start}, c_end={c_end})"
            )
            cursor.execute(q_comm_pr,(repo, c_start, c_end))
            row_data['commentsPRRaw'] = cursor.fetchone()[0]

            q_react_pr = """
              SELECT COUNT(*)
              FROM comment_reactions r
              JOIN pulls p ON p.repo_name=r.repo_name AND p.pull_number=r.issue_number
              WHERE r.repo_name=%s
                AND r.created_at >= %s
                AND r.created_at < %s
            """
            debug_lines.append(
              f"[SQL-Query reactPR for {repo} from {c_start} to {c_end}]\n"
              + q_react_pr.replace('\n',' ') + "\n"
              + f"[params] => (repo={repo}, c_start={c_start}, c_end={c_end})"
            )
            cursor.execute(q_react_pr,(repo, c_start, c_end))
            row_data['reactPRRaw'] = cursor.fetchone()[0]

            # distinct participants or other advanced logic ...
            row_data['distinctPartRaw'] = 0  # placeholders, or do a real query

        except Exception as ex:
            msg = f"[ERROR] splitted_metrics => gather data for {repo} from {c_start} to {c_end}, ex={ex}"
            debug_lines.append(msg)
            # we can either raise or continue with partial data
            raise

        results.append(row_data)

    return results
