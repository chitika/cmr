_lw_complete()
{
    local cur prev
    _get_comp_words_by_ref -n : cur prev
    ARR=($(lw "--bash-complete" "$cur" "$COMP_TYPE"))
    COMPREPLY=($(compgen -W "$(printf "%s " "${ARR[@]}")" -- "$word"))
    __ltrim_colon_completions "$cur"
}

complete -F _lw_complete lw
alias lw='set +B -f;lw';lw(){ command lw "$@";set -B +f;}

_lcat_complete()
{
    local cur prev
    _get_comp_words_by_ref -n : cur prev
    ARR=($(lcat "--bash-complete" "$cur" "$COMP_TYPE"))
    COMPREPLY=($(compgen -W "$(printf "%s " "${ARR[@]}")" -- "$word"))
    __ltrim_colon_completions "$cur"
}

complete -F _lcat_complete lcat
alias lcat='set +B -f;lcat';lcat(){ command lcat "$@";set -B +f;}

