# PastryWordCount

##For Pinchao

这是WordCount的Code。
Testentry类负责生成每个VM中的Node，并让他们Scribe，计算。
WordcountApplication是具体的Scribe Client，负责处理数据与给出最后输出。
Msg是消息类，包括了所有在Pastry中传递的SCribe Content 和 Message
