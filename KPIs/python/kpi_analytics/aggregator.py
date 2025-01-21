# aggregator.py

def velocity(merges, closed_issues):
    return 0.4 * merges + 0.6 * closed_issues

def user_interest_growth(forks, stars):
    return 0.4 * forks + 0.6 * stars

def monthly_active_contributors(new_issues, new_comments, new_reactions, new_pulls):
    """
    MAC = 0.8*(new_issues + new_comments + new_reactions)
          + 0.2*(new_pulls)
    """
    icr = new_issues + new_comments + new_reactions
    return 0.8 * icr + 0.2 * new_pulls
