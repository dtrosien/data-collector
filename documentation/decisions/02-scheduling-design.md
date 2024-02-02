# Scheduling design

- [Status](#status)
- [Context](#context)
- [Options](#options)
	- [Layer based scheduling](#layer-based-scheduling)
		- [Pro](#pro)
		- [Contra](#contra)
	- [DAG based scheduling](#dag-based-scheduling)
		- [Pro](#pro)
		- [Contra](#contra)
- [Decision](#decision)
- [Consequences](#consequences)

## Status

In progress

## Context

The project now contains tasks which are separated in <i>collecting</i> data from sources and <i>staging</i> them (short: collect;stage). Naturally the data should be first collected and then staged, this brings a dependency to the existing tasks.</br>
Scheduling in this context means executing tasks in a certain order, meaning tasks can be executed in parallel or after each other (Here we are not talking about time based scheduling, like start job x at time y).


## Options

For both options the user should still have control over which tasks should be executed, so the include and exclude logic within the config file stays unchanged. The discussed options are the <i>layer</i>-based or the <i>DAG</i>-based scheduling.

### Layer based scheduling

The layer based scheduling is the currently implemented version (2024.01.28). Layers are an abstract concept for ordering tasks
- each layer contains 1 or more tasks
- a task can be in multiple layers, so it will be executed more often
- starting/executing a layer will start all tasks in parallel assigned to the layer
- layers have assigned integers
- layers are executed with ascending order; so layer 0 will be executed before layer 1
- a layer is started if the previous layer has finished executing

The assignment of tasks to the layers will be organized in the config file.

#### Pro
This is already implemented.<br/>
Dependencies can be easily modelled by the user.<br/>


#### Contra
User has to have some knowledge about all the jobs in order to design the dependencies correct.<br/>
Long running tasks will either block other tasks or have to be put at a layer at the end.<br/>
One non finishing task in a layer will block all subsequent tasks.


### DAG based scheduling

The DAG (directed acyclic graph) based scheduling consists of a predefined DAG within the application. <br/>
Each tasks corresponds to an vertex inside the DAG. If the task A must be executed before task B, then the edge (A,B) will be added. Each vertex with an in-degree of 0 can be executed at once, so tasks will run in parallel. A finished task will remove the corresponding vertex from the graph, subsequent vertices with in-degree 0 can be started directly.<br/>
Having not all tasks selected for an application execution will be reflected by computing an induced subgraph, using only the selected tasks. The new graph will be used as the base graph for that run.

#### Pro
Fast spawning of tasks, since only waiting on the direct dependencies is required.<br/>
User don't have to have a deep knowledge of the dependencies, since they are hardcoded.
One misbehaving job will only block descendants.

#### Contra
Hard to add new tasks to the application, since the dependency should be reflected.<br/>
Dependencies are hard coded, so changing them requires to recompile the application.<br/>
Additional effort to implement the solution, while having very little gain from it right now.


## Decision

The <i>layer</i>-based scheduling will be picked. Main driver for the decision is that it is already implemented and the DAG-based solution requires additional effort, while having little gain from it. Currently there are only 5 jobs implemented and a full run finishes within 2 minutes, depending on the internet speed.<br/>
In the future this decision might be reconsidered, if the gain from the DAG-based scheduling increases or implementation effort will decrease.


## Consequences

No immediate action is needed.</br>
The config file will stay the same and the layer config variable can stay.</br>
The module for the scheduler should stay independent from other parts of the code, so it can easily be replaced in the future.
Developer and user # Scheduling design

- [Status](#status)
- [Context](#context)
- [Options](#options)
	- [Layer based scheduling](#layer-based-scheduling)
		- [Pro](#pro)
		- [Contra](#contra)
	- [DAG based scheduling](#dag-based-scheduling)
		- [Pro](#pro)
		- [Contra](#contra)
- [Decision](#decision)
- [Consequences](#consequences)

## Status

In progress

## Context

The project now contains tasks which are separated in <i>collecting</i> data from sources and <i>staging</i> them (short: collect;stage). Naturally the data should be first collected and then staged, this brings a dependency to the existing tasks.</br>
Scheduling in this context means executing tasks in a certain order, meaning tasks can be executed in parallel or after each other (Here we are not talking about time based scheduling, like start job x at time y).


## Options

For both options the user should still have control over which tasks should be executed, so the include and exclude logic within the config file stays unchanged. The discussed options are the <i>layer</i>-based or the <i>DAG</i>-based scheduling.

### Layer based scheduling

The layer based scheduling is the currently implemented version (2024.01.28). Layers are an abstract concept for ordering tasks
- each layer contains 1 or more tasks
- a task can be in multiple layers, so it will be executed more often
- starting/executing a layer will start all tasks in parallel assigned to the layer
- layers have assigned integers
- layers are executed with ascending order; so layer 0 will be executed before layer 1
- a layer is started if the previous layer has finished executing

The assignment of tasks to the layers will be organized in the config file.

#### Pro
This is already implemented.<br/>
Dependencies can be easily modelled by the user.<br/>


#### Contra
User has to have some knowledge about all the jobs in order to design the dependencies correct.<br/>
Long running tasks will either block other tasks or have to be put at a layer at the end.<br/>
One non finishing task in a layer will block all subsequent tasks.


### DAG based scheduling

The DAG (directed acyclic graph) based scheduling consists of a predefined DAG within the application. <br/>
Each tasks corresponds to an vertex inside the DAG. If the task A must be executed before task B, then the edge (A,B) will be added. Each vertex with an in-degree of 0 can be executed at once, so tasks will run in parallel. A finished task will remove the corresponding vertex from the graph, subsequent vertices with in-degree 0 can be started directly.<br/>
Having not all tasks selected for an application execution will be reflected by computing an induced subgraph, using only the selected tasks. The new graph will be used as the base graph for that run.

#### Pro
Fast spawning of tasks, since only waiting on the direct dependencies is required.<br/>
User don't have to have a deep knowledge of the dependencies, since they are hardcoded.
One misbehaving job will only block descendants.

#### Contra
Hard to add new tasks to the application, since the dependency should be reflected.<br/>
Dependencies are hard coded, so changing them requires to recompile the application.<br/>
Additional effort to implement the solution, while having very little gain from it right now.


## Decision

The <i>layer</i>-based scheduling will be picked. Main driver for the decision is that it is already implemented and the DAG-based solution requires additional effort, while having little gain from it. Currently there are only 5 jobs implemented and a full run finishes within 2 minutes, depending on the internet speed.<br/>
In the future this decision might be reconsidered, if the gain from the DAG-based scheduling increases or implementation effort will decrease.


## Consequences

No immediate action is needed.</br>
The config file will stay the same and the layer config variable can stay.</br>
The module for the scheduler should stay independent from other parts of the code, so it can easily be replaced in the future.
Developer and user 