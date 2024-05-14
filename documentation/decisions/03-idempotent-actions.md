# Actions must be idempotent

## Status

In progress

## Context

Actions are entities in this project, which can be executed and are adding to a business logic. Right now, there are <i>two kind</i> of actions: Collectors and Stagers. They are either collecting data from sources or integrate that gathered data into business tables.<br/>
Idempotency is a mathematical concept about functions $f\colon A \rightarrow A$ mapping from a domain $A$ to itself. An function $f$ is called idempotent if $f(x) = f(f(x))$, meaning that iterated execution of $f$ yields the same result.</br>
In our context $f$ will be one of the actions and $A$ is either the an online interface or the state of the database.</br>
 - In the case of the internet, we will just assume that the state of the internet is frozen (just for the sake of the definition), so executing an action twice means that an API will responde with the identical two answers. Practically this means that receiving identical data will not lead to a pile up of duplicates.
 - In the case of the database we have a predefined state $x$ and call $f$ two times. Here $f$ will not pile up data in the database or reverse its own action on the second execution.</br>
</br>
Do not confuse this with abort-ability of functions. We assume that $f$ terminates in a valid way.



## Options

### Actions are not idempotent

Actions must not be idempotent.

### Actions are idempotent

Actions must be idempotent.


## Decision

Actions must be idempotent.


## Consequences

Actions must be implemented in an idempotent way.</br>
It is desired to make this testable, but now unknown how to achieve this.</br>
Developers/users of the application do not have to take care about executing the application multiple times.</br>
No pile up of undesired duplicates in the database.</br>
