from enum import Enum
from functools import lru_cache


class QrelDeduplication(Enum):
    GLOBAL_QREL_DEDUPLICATION = 1
    RUN_LOCAL_QREL_DEDUPLICATION = 2


class Measure(Enum):
    MAP = 1
    NDCG = 2


class SharedTasks(Enum):
    TERABYTE_2004 = 1
    TERABYTE_2005_ADHOC = 2
    TERABYTE_2006 = 3
    WEB_2009 = 4
    WEB_2010 = 5
    WEB_2011 = 6
    WEB_2012 = 7
    WEB_2013 = 8
    WEB_2014 = 9
    CORE_2017 = 10
    CORE_2018 = 11


QREL_MANIPULATIION_DISPLAY_VALUES = {
    QrelDeduplication.GLOBAL_QREL_DEDUPLICATION.name: 'maxValueAllDuplicateDocsIrrelevant',
    QrelDeduplication.RUN_LOCAL_QREL_DEDUPLICATION.name: 'maxValueDuplicateDocsIrrelevant'
}


SHARED_TASK_DISPLAY_VALUES = {
    'TERABYTE_2004': 'TB 04',
    'TERABYTE_2005_ADHOC': 'TB 05',
    'TERABYTE_2006': 'TB 06',
    'WEB_2009': 'Web 09',
    'WEB_2010': 'Web 10',
    'WEB_2011': 'Web 11',
    'WEB_2012': 'Web 12',
    'WEB_2013': 'Web 13',
    'WEB_2014': 'Web 14',
    'CORE_2017': 'Core 17',
    'CORE_2018': 'Core 18',
}

def shared_task_judgement_inconsistencies():
    import json
    sharedTasksJudgementInconsistencies = {}
    for json_report in ['../intermediate-results/judgment-inconsistencies-clueweb.jsonl', '../intermediate-results/judgment-inconsistencies-gov.jsonl', '../intermediate-results/judgment-inconsistencies-core.jsonl']:
        with open(json_report, 'r') as f:
            for line in f:
                jsonParsed = json.loads(line)
                sharedTasksJudgementInconsistencies[jsonParsed["SharedTask"]] = jsonParsed
    return sharedTasksJudgementInconsistencies

def shared_task_evaluations():
    import json
    #for json_report in ['../gov-evaluation.jsonl', '../core2017-evaluation.jsonl', '../core2018-evaluation.jsonl']:
    #for json_report in ['../intermediate-results/core2018-evaluation-0_58.jsonl']:
    #for json_report in ['../intermediate-results/clueweb12-evaluation-0_58.jsonl']:
    for json_report in ['../intermediate-results/gov-evaluation.jsonl', '../intermediate-results/clueweb09-evaluation.jsonl', '../intermediate-results/clueweb12-evaluation.jsonl', '../intermediate-results/core2017-evaluation.jsonl', '../intermediate-results/core2018-evaluation.jsonl']:
        with open(json_report, 'r') as f:
            for line in f:
                yield json.loads(line)


def f(number):
    return "{0:.5f}".format(number)


def content_equivalent_in_relevant(topic):
    if(topic['relevantJudgments'] == 0):
        return 0

    return (topic['contentEquivalentDocsInRelevantJudgments'] - topic['contentEquivalentGroupsInRelevantJudgments'])/topic['relevantJudgments']


def retrieval_equivalent_in_relevant(topic):
    if(topic['relevantJudgments'] == 0):
        return 0

    return (topic['retrievalEquivalentDocsInRelevantJudgments'] - topic['retrievalEquivalentGroupsInRelevantJudgments'])/topic['relevantJudgments']


def content_equivalent_in_irrelevant(topic):
    return (topic['contentEquivalentDocsInIrrelevantJudgments'] - topic['contentEquivalentGroupsInIrrelevantJudgments'])/topic['irrelevantJudgments']


def retrieval_equivalent_in_irrelevant(topic):
    return (topic['retrievalEquivalentDocsInIrrelevantJudgments'] - topic['retrievalEquivalentGroupsInIrrelevantJudgments'])/topic['irrelevantJudgments']

def raw_judgment_evaluations():
    import json
    for judgment_file in ['../intermediate-results/judgment-inconsistencies-clueweb.jsonl', '../intermediate-results/judgment-inconsistencies-core.jsonl', '../intermediate-results/judgment-inconsistencies-gov.jsonl']:
        with open(judgment_file, 'r') as f:
            for line in f:
                yield json.loads(line)


def get_judgment_details(shared_task):
    judgments = [i for i in raw_judgment_evaluations() if i['SharedTask'] == shared_task]
    if len(judgments) != 1:
        raise ValueError('Fix the handling of ' + shared_task)
    return judgments[0]


def sorted_ranking(ranking):
    return sorted([(i.split('/')[-1],j) for (i,j) in ranking.items()], key=lambda i: -1*i[1])

def ranking_changes(evaluation):
    ret = {}
    original = evaluation['OriginalRanking']
    manipulated = evaluation['ManipulatedRanking']

    for retrieval_system in original.keys():
        ret[retrieval_system] = {
            'originalScore': original[retrieval_system],
            'manipulatedScore': manipulated[retrieval_system],
            'changeInPercent': -100* ((original[retrieval_system] - manipulated[retrieval_system])/original[retrieval_system]),
        }
    return ret

def ranking_changes_for(sharedTask: SharedTasks, dedup, measure: Measure, qrel_deduplication: QrelDeduplication):
    ret = evaluation_for(sharedTask, dedup, measure, qrel_deduplication)['RankingChanges']
    return sorted([ret[i] for i in ret.keys()], key=lambda i: -i['originalScore'])

def calculate_shared_task_evaluation(evaluation):
    import statistics
    scoreChanges = evaluation['ScoreChangesPerRank']
    avgEval = evaluation['AverageRankEval']
    #print(evaluation.keys())

    #print(sorted_ranking(evaluation['OriginalRanking']))
    #print(sorted_ranking(evaluation['ManipulatedRanking']))

    return {
        'EvaluationMeasure': evaluation['EvaluationMeasure'],
        'Deduplication': evaluation['Deduplication'].replace('Duplicates',''),
        'QrelConsistency': evaluation['QrelConsistency'],
        'SystemRankingPreprocessing': evaluation['SystemRankingPreprocessing'],
        'KendallTau': f(evaluation['KendallTau']),
        'KendallTau@5': f(evaluation['KendallTau@5']),
        'KendallTau@10': f(evaluation['KendallTau@10']),
        'Average(Official-Score)': avgEval['avgOriginal'],
        'Average(New-Score)': f(avgEval['avgManipulated']),
        'Delta(Official vs New)': f(avgEval['difference']),
        'Delta(Official vs New %)': f((avgEval['difference']/avgEval['avgOriginal'])*100),
        'Min(Score-Delta)': f(min(scoreChanges)),
        'Median(Score-Delta)': f(statistics.median(scoreChanges)),
        'Max(Score-Delta)': f(max(scoreChanges)),
	'ScoreChangesPerRank': scoreChanges,
        'RankingChanges': ranking_changes(evaluation),
    }


def is_evaluation_for_idealized_participants(evaluation):
    return 'remove' == evaluation['Deduplication'].replace('Duplicates','') and 'base' == evaluation['QrelConsistency'] and 'DISCARD_LAST_PERCENT' == evaluation['SystemRankingPreprocessing']


def losses_for_idealized_participants():
    import statistics
    ret = {}
    for evaluation in shared_task_evaluations():
        if not(is_evaluation_for_idealized_participants(evaluation)):
            continue
        evaluationMeasure = evaluation['EvaluationMeasure']

        if evaluationMeasure not in ret:
            ret[evaluationMeasure] = {}
        if evaluation['SharedTask'] in ret[evaluationMeasure]:
            raise ValueError('Unexpected unambiguoity...')

        ideal = evaluation['IdealParticipantVector']
        ret[evaluationMeasure][evaluation['SharedTask']] = {
            'Min-Idealized': str(min(ideal)),
            'Median-Idealized': str(statistics.median(ideal)),
            'Max-Idealized': str(max(ideal)),
        }
    return ret


def evaluation_for(shared_task: SharedTasks, dedup, measure: Measure, qrel_deduplication: QrelDeduplication):
    ret = []
    for evaluation in shared_task_evaluations():
        if shared_task.name == evaluation['SharedTask'] and dedup == evaluation['Deduplication'].replace('Duplicates','') and measure.name == evaluation['EvaluationMeasure'] and QREL_MANIPULATIION_DISPLAY_VALUES[qrel_deduplication.name] == evaluation['QrelConsistency'] and 'DISCARD_LAST_PERCENT' == evaluation['SystemRankingPreprocessing']:
            ret += [calculate_shared_task_evaluation(evaluation)]
    if len(ret) != 1:
        raise ValueError('Expected 1, got ' + str(len(ret)) +' matches... for ' + str([shared_task, dedup, measure, qrel_deduplication]))

    return ret[0]


def table_3_row(shared_task: SharedTasks, measure: Measure, qrel_deduplication: QrelDeduplication):
    losses_for_idealized = losses_for_idealized_participants()[measure.name][shared_task.name]
    remove_duplicates = evaluation_for(
        shared_task=shared_task,
        dedup='remove',
        measure=measure,
        qrel_deduplication=qrel_deduplication,
    )
    duplicates_irrelevant = evaluation_for(
        shared_task=shared_task,
        dedup='duplicatesMarkedIrrelevant',
        measure=measure,
        qrel_deduplication=qrel_deduplication,
    )

    return '|' + SHARED_TASK_DISPLAY_VALUES[shared_task.name] + '|' \
               + losses_for_idealized['Min-Idealized'] + '|'+ losses_for_idealized['Median-Idealized'] +'|' + losses_for_idealized['Max-Idealized'] + '|' \
               + f(remove_duplicates['Average(Official-Score)']) +'|' \
               + duplicates_irrelevant['Delta(Official vs New %)'] + '|' + duplicates_irrelevant['KendallTau'] + '|' + duplicates_irrelevant['KendallTau@5'] + '|' \
               + remove_duplicates['Delta(Official vs New %)'] + '|' + remove_duplicates['KendallTau'] + '|' + remove_duplicates['KendallTau@5'] + '|'


def table_3(measure:Measure, qrel_deduplication: QrelDeduplication):
    from IPython.display import display, Markdown
    ret = '# Table 3 (' + measure.name + '; ' + qrel_deduplication.name + ')\n'
    ret += '\n|Shared Task|Min-Ideal|Median-Ideal|Max-Ideal|Avg-' + measure.name + '|Duplicates-Irrelevant Delta_{' + measure.name + '} (%)|Duplicates-Irrelevant Kendall-Tau|Duplicates-Irrelevant Kendall-Tau@5|Duplicates-Removed Delta_{' + measure.name + '} (%)|Duplicates-Removed Kendall-Tau|Duplicates-Removed Kendall-Tau@5|'+\
           '\n|:----------|--------:|-----------:|--------:|-------:|---------------------:|---------------:|-----------------:|---------------------:|---------------:|-----------------:|\n'
    ret += '\n'.join([table_3_row(shared_task, measure, qrel_deduplication) for shared_task in SharedTasks])
    return display(Markdown(ret))


def plot_redundant_docs_in_relevant_docs():
    import matplotlib.pyplot as plt
    import numpy as np
    from IPython.display import Image
    retrieval_equivalent = []
    content_equivalent = []

    for shared_task in SharedTasks:
        topic_details = get_judgment_details(shared_task.name)['TopicDetails']
        retrieval_equivalent += [[retrieval_equivalent_in_relevant(topic_details[i])*100 for i in topic_details.keys()]]
        content_equivalent += [[content_equivalent_in_relevant(topic_details[i])*100 for i in topic_details.keys()]]

    def set_box_color(bp, color):
        plt.setp(bp['boxes'], color=color)
        plt.setp(bp['whiskers'], color=color)
        plt.setp(bp['caps'], color=color)
        plt.setp(bp['medians'], color=color)

    plt.figure(figsize=(9, 4))

    bpl = plt.boxplot(retrieval_equivalent, positions=np.array(range(len(retrieval_equivalent)))*2.0-0.4, sym='', widths=0.6, showfliers=True)
    bpr = plt.boxplot(content_equivalent, positions=np.array(range(len(content_equivalent)))*2.0+0.4, sym='', widths=0.6, showfliers=True)
    set_box_color(bpl, 'grey')
    set_box_color(bpr, 'black')

    plt.plot([], c='grey', label='Retrieval-Equivalent')
    plt.plot([], c='black', label='Content-Equivalent')
    plt.legend()

    plt.xticks(range(0, len(SharedTasks) * 2, 2), [SHARED_TASK_DISPLAY_VALUES[i.name] for i in SharedTasks])
    plt.tick_params(labelright=True)
    plt.xlim(-2, len(SharedTasks)*2)
    plt.ylabel('Redundant Documents (%)')
    plt.xlabel('TREC Tracks')
    plt.ylim(0, 75)
    plt.tight_layout()
    plt.savefig('redundant-docs-in-relevant-docs.eps', format='eps')
    plt.savefig('redundant-docs-in-relevant-docs.png')
    plt.close()

    return Image('redundant-docs-in-relevant-docs.png')

def plot_redundant_docs_in_irrelevant_docs():
    import matplotlib.pyplot as plt
    from IPython.display import Image
    import numpy as np

    retrieval_equivalent = []
    content_equivalent = []

    for shared_task in SharedTasks:
        topic_details = get_judgment_details(shared_task.name)['TopicDetails']
        retrieval_equivalent += [[retrieval_equivalent_in_irrelevant(topic_details[i])*100 for i in topic_details.keys()]]
        content_equivalent += [[content_equivalent_in_irrelevant(topic_details[i])*100 for i in topic_details.keys()]]

    def set_box_color(bp, color):
        plt.setp(bp['boxes'], color=color)
        plt.setp(bp['whiskers'], color=color)
        plt.setp(bp['caps'], color=color)
        plt.setp(bp['medians'], color=color)

    plt.figure(figsize=(9, 4))

    bpl = plt.boxplot(retrieval_equivalent, positions=np.array(range(len(retrieval_equivalent)))*2.0-0.4, sym='', widths=0.6, showfliers=True)
    bpr = plt.boxplot(content_equivalent, positions=np.array(range(len(content_equivalent)))*2.0+0.4, sym='', widths=0.6, showfliers=True)
    set_box_color(bpl, 'grey')
    set_box_color(bpr, 'black')

    plt.plot([], c='grey', label='Retrieval-Equivalent')
    plt.plot([], c='black', label='Content-Equivalent')
    plt.legend()

    plt.xticks(range(0, len(SharedTasks) * 2, 2), [SHARED_TASK_DISPLAY_VALUES[i.name] for i in SharedTasks])
    plt.xlim(-2, len(SharedTasks)*2)
    plt.ylabel('Redundant Documents (%)')
    plt.xlabel('TREC Tracks')
    plt.ylim(0, 75)
    plt.tight_layout()
    plt.savefig('redundant-docs-in-irrelevant-docs.png')
    plt.close()

    return Image('redundant-docs-in-irrelevant-docs.png')


def judgment_table_row(evaluation):
    return '|' + evaluation['SharedTask'] + '|' + str(evaluation['ConsistentGroups']) +\
           '|' + str(evaluation['InconsistentGroups']) +\
           '|' + str(evaluation['InconsistentGroups(WithoutUnlabeled)']) +\
           '|' + f(int(evaluation['InconsistentGroups'])/int(evaluation['ConsistentGroups'])) +\
           '|' + str(evaluation['JudgmentCount']) + '|' + str(evaluation['DocumentsToJudgeAgain']) +\
           '|' + f(int(evaluation['InconsistentGroups'])/int(evaluation['JudgmentCount']))


def judgment_table():
    table_header = '|SharedTask|ConsistentGroups|InconsistentGroups|InconsistentGroups(Without Unlabeled)|Ratio (inconsistent/consistent)|Judgments|Rejudgments|Ratio(inconsistent/judged)|\n' + \
           '|:---------|---------------:|-----------------:|------------------------------:|---------------------------:|--------:|----------:|-------------------------:|\n'

    return table_header + '\n'.join((judgment_table_row(i) for i in raw_judgment_evaluations())) + '\n\n'


def topic_level_judgment_details_table(topics):
    ret = '|Topic|Relevant Docs|Content-Equivalent|Retrieval-Equivalent|Irrelevant Docs|Content-Equivalent|Retrieval-Equivalent|\n' + \
          '|:----|------------:|-----------------:|-------------------:|--------------:|-----------------:|-------------------:|\n'

    for topic in sorted(topics.keys()):
        d = topics[topic]
        relevant = d['relevantJudgments']
        irrelevant = d['irrelevantJudgments']
        ret += '|' + str(topic) +\
               '|' + str(relevant) + '|' + f(content_equivalent_in_relevant(d)) + '|' + f(retrieval_equivalent_in_relevant(d)) + \
               '|' + str(d['irrelevantJudgments']) + '|' + f(content_equivalent_in_irrelevant(d)) + '|' + f(retrieval_equivalent_in_irrelevant(d)) + '|\n'

    return ret +'\n'


def topic_level_judgment_details(shared_task: SharedTasks):
    from IPython.display import display, Markdown, Latex
    judgment = get_judgment_details(shared_task.name)
    display(Markdown(topic_level_judgment_details_table(judgment['TopicDetails'])))

def inconsistencies_within_judgments():
    from IPython.display import display, Markdown, Latex
    display(Markdown(judgment_table()))

def plot_per_rank_score_changes(shared_tasks, measure: Measure, qrel_deduplication: QrelDeduplication):
    import matplotlib.pyplot as plt
    plt.rcParams.update({'font.size': 13})
    from IPython.display import Image

    fig, (equivalent_irrelevant, equivalent_removed) = plt.subplots(ncols=2, figsize=(15, 5))

    equivalent_removed.set_ylabel(measure.name + '-Changes (%)')
    equivalent_removed.set_xlabel('TREC Submission')
    equivalent_removed.set_title('Equivalent Documents removed')
    equivalent_removed.set_xticks(range(5,50, 5))
    equivalent_removed.set_ylim(-45, 10)
    #equivalent_removed.set_yscale('linear')

    equivalent_irrelevant.set_ylabel(measure.name + '-Changes (%)')
    equivalent_irrelevant.set_xlabel('TREC Submission')
    equivalent_irrelevant.set_xticks(range(5,50, 5))
    equivalent_irrelevant.set_title('Equivalent Documents marked irrelevant')
    equivalent_irrelevant.set_ylim(-45, 10)
    #equivalent_irrelevant.set_yscale('symlog')

#    color_cycle = plt.rcParams['axes.prop_cycle']()
    color_cycle = (i for i in ['red', 'black', 'yellow'])

    width = 1/(len(shared_tasks))
    offset = -width
    for shared_task in shared_tasks:
        removed_data = [i['changeInPercent'] for i in ranking_changes_for(shared_task, 'remove', measure, qrel_deduplication)]
        marked_irrelevant_data = [i['changeInPercent'] for i in ranking_changes_for(shared_task, 'duplicatesMarkedIrrelevant', measure, qrel_deduplication)]
        n = {'color': next(color_cycle)}
        positions = [i + offset for i in range(len(removed_data))]
        equivalent_removed.bar(positions, removed_data, width=width, label=SHARED_TASK_DISPLAY_VALUES[shared_task.name], **n)
        equivalent_irrelevant.bar(positions, marked_irrelevant_data, width=width, label=SHARED_TASK_DISPLAY_VALUES[shared_task.name], **n)
        offset += width

    lines_labels = [equivalent_removed.get_legend_handles_labels()]
    lines, labels = [sum(lol, []) for lol in zip(*lines_labels)]

#    fig.subplots_adjust(wspace=.5)
    fig.legend(lines, labels, loc=(0.35,0.15))
    fig.legend(lines, labels, loc=(0.84,0.15))

    plot_name = '-'.join([i.name for i in shared_tasks]) + '-' + measure.name + '-' + qrel_deduplication.name + '-score-changes'
    plt.savefig(plot_name + '.eps', format='eps', bbox_inches='tight')
    plt.savefig(plot_name + '.png', bbox_inches='tight')
    plt.close()
    return Image(plot_name + '.png')



def proportion_of_redundant_documents_per_topic(labels, top_small, top_big, bottom_small, bottom_big, plot_name, title=""):
    import matplotlib
    import matplotlib.pyplot as plt
    import numpy as np
    from IPython.display import Image
    height = 1

    labels, top_small, top_big, bottom_small, bottom_big = (list(t) for t in zip(*sorted(zip([int(i) for i in labels], top_small, top_big, bottom_small, bottom_big))))


    for idx, val in enumerate(top_small):
        top_small[idx]=0-top_small[idx]

    for idx, val in enumerate(top_big):
        top_big[idx]=0-top_big[idx]-top_small[idx]

    for idx, val in enumerate(bottom_big):
        bottom_big[idx]=bottom_big[idx]-bottom_small[idx]

    top_big_minus_height=list(range(0,len(top_big)))
    for idx, val in enumerate(top_big):
        top_big_minus_height[idx]=top_small[idx]+height

    x = np.arange(len(labels))  # the label locations
    width = 0.8  # the width of the bars
    fig, ax = plt.subplots(figsize=(8,3.5))
    min_max_label = [-10, len(labels)+10]
    plt.plot(min_max_label,[0.25,0.25], color="grey", linestyle=":")
    plt.plot(min_max_label,[height-0.25,height-0.25], color="grey", linestyle=":")
    rects1 = ax.bar(x , top_small, width,label='Irrelevant documents (RE only)', bottom=height, color="#F5793A")
    rects3 = ax.bar(x , top_big, width, label='Irrelevant documents',bottom=top_big_minus_height, color="#A95AA1")
    rects2 = ax.bar(x , bottom_small, width,label='Relevant documents (RE only)',  bottom=0, color="#85C0F9")
    rects4 = ax.bar(x , bottom_big, width, label='Relevant documents',bottom=bottom_small, color="#0F2080")
    plt.yticks([0,0.1,0.20,0.30,0.40,0.50,height], [0,10,20,30,40,50,""])
    ax.set_xbound([-1,len(labels)])
    ax2 = ax.twinx()
    ax2.set_ylabel('',rotation=-90)
    plt.text(len(labels)+3,0,'Redundant Documents (%)                     ',rotation=-90,horizontalalignment="right", rotation_mode="anchor")
    plt.yticks([height,height-0.10,height-0.20,height-0.30,height-0.40,height-0.50,0], [0,10,20,30,40,50,""])
    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('Redundant Documents (%)')
    used_label_names=[]
    used_label_positions=[]
    used_label_names.append(labels[0])
    used_label_positions.append(0)
    for idx, val in enumerate(labels):
        if int(val)%5==0:
            used_label_names.append(val)
            used_label_positions.append(idx)
    plt.xticks(used_label_positions, used_label_names)
    ax.set_xlabel('Topic')
    ax.set_title(title)
    ax.legend()
    plt.savefig(plot_name)
    plt.close()
    return Image(plot_name + '.png')

def map_on_queries(runs, base, removed, marked,plot_name, ylab=""):
    import matplotlib.pyplot as plt
    from IPython.display import Image
    fig, ax = plt.subplots(figsize=(16, 5))
    ax.set_ylim(bottom=0, top=max(base+removed+marked)+0.05)
    ax.set_xlim(left=0.5, right=len(runs)+0.5)
    ax.scatter(range(1,len(runs)+1), base, s=48, c='grey', marker="o", label='TREC result')
    ax.scatter(range(1,len(runs)+1),removed, s=48, marker="o", label='Equivalent docs removed', facecolors='white', edgecolors='black')
    ax.scatter(range(1,len(runs)+1),marked, s=48, c='black', marker="o", label='Equivalent docs marked irrelevant')

    used_label_names=[]
    used_label_positions=[]
    for idx in range(0,len(runs)):
        if idx%5==0:
            used_label_names.append(idx)
            used_label_positions.append(idx)
    plt.xticks(used_label_positions, used_label_names)
    ax.set_ylabel(ylab)
    ax.set_xlabel('TREC Submission')
    plt.legend(loc='upper right');
    plt.savefig(plot_name)
    #plt.show()
    plt.close()
    return Image(plot_name + '.png')

def plot_proportion_of_redundant_documents_per_topic(sharedTask: SharedTasks):
    x = []
    relevant_base = []
    relevant_only_re = []
    irrelevant_base = []
    irrelevant_only_re = []
    for key, value in shared_task_judgement_inconsistencies()[sharedTask.name]["TopicDetails"].items():
        x.append(key)
        relevant_base.append(content_equivalent_in_relevant(value))
        relevant_only_re.append(retrieval_equivalent_in_relevant(value))
        irrelevant_base.append(content_equivalent_in_irrelevant(value))
        irrelevant_only_re.append(retrieval_equivalent_in_irrelevant(value))
    return proportion_of_redundant_documents_per_topic(labels=x, top_small=irrelevant_only_re, top_big=irrelevant_base, bottom_small=relevant_only_re, bottom_big=relevant_base, title="Proportion of redundant documents by topic in "+sharedTask.name, plot_name="plot_proportion_of_redundant_documents_by_topic_"+sharedTask.name)


def plot_map_on_queries(sharedTask: SharedTasks, measure: Measure, qrel_deduplication: QrelDeduplication):
    import matplotlib.pyplot as plt
    #evaluation_base=evaluation_for(sharedTask, "base", measure, qrel_deduplication)["RankingChanges"]
    evaluation_removed=evaluation_for(sharedTask, "remove", measure, qrel_deduplication)["RankingChanges"]
    evaluation_marked=evaluation_for(sharedTask, "duplicatesMarkedIrrelevant", measure, qrel_deduplication)["RankingChanges"]
    sorted_systems = sorted(evaluation_marked.keys(),key=lambda i: evaluation_marked[i]['originalScore'], reverse=True)
    runs = []
    base = []
    marked = []
    removed = []
    for system in sorted_systems:
        runs.append(system.split("/")[-1])
        base.append(evaluation_marked[system]['originalScore'])
        marked.append(evaluation_marked[system]['manipulatedScore'])
        removed.append(evaluation_removed[system]['manipulatedScore'])
    return map_on_queries(runs=runs, base=base, removed=removed, marked=marked, plot_name="plot_map_on_queries", ylab=measure.name)
