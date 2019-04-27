/* 3, 201828015059015, YunHao */
/**
 * @file 3_201828015059015_hw2.cc
 * @author  yunhao
 * @version 0.1
 *
 * @section DESCRIPTION
 *
 * Solution for Big Data Homework 2 Part 2.
 *
 */

#include <iostream>
#include <malloc.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include <algorithm>

#include "GraphLite.h"

using namespace std;

#define VERTEX_CLASS_NAME(name) DirectedTriangleCount##name

/** Vertex value type. */
struct DTCVertexValue {
    /** Store the four statistical value of current vertex. */
    int res[4];
};

/** Message value type. */
struct DTCMessageValue {
    /** from -> to is the tuple of edge vertex. */
    int from, to;

    /**
     * Operator < function rewrite.
     * @param b The other operand during comparison.
     * @return Boolean value for result of comparison.
     */
    bool operator < (DTCMessageValue& b) {
        if(from == b.from) return to < b.to;
        return from < b.from;
    }
    /**
     * Operator > function rewrite.
     * @param b The other operand during comparison.
     * @return Boolean value for result of comparison.
     */
    bool operator > (DTCMessageValue& b) {
        if(from == b.from) return to > b.to;
        return from > b.from;
    }
    /**
     * Operator == function rewrite.
     * @param b The other operand during comparison.
     * @return Boolean value for result of comparison.
     */
    bool operator == (DTCMessageValue& b) {
        return from == b.from && to == b.to;
    }
};

/**
 * InputFormatter class. Read file for creating graphs.
 */
class VERTEX_CLASS_NAME(InputFormatter): public InputFormatter {
public:
    /**
     * Get the number of vertex.
     * @return int value of the number.
     */
    int64_t getVertexNum() {
        unsigned long long n;
        sscanf(m_ptotal_vertex_line, "%lld", &n);
        m_total_vertex= n;
        return m_total_vertex;
    }
    /**
     * Get the number of edge.
     * @return int value of the number.
     */
    int64_t getEdgeNum() {
        unsigned long long n;
        sscanf(m_ptotal_edge_line, "%lld", &n);
        m_total_edge= n;
        return m_total_edge;
    }
    /**
     * Get the value size of vertex.
     * @return int value of size.
     */
    int getVertexValueSize() {
        m_n_value_size = sizeof(DTCVertexValue);
        return m_n_value_size;
    }
    /**
     * Get the value size of edge.
     * @return int value of size.
     */
    int getEdgeValueSize() {
        m_e_value_size = sizeof(double);
        return m_e_value_size;
    }
    /**
     * Get the value size of message.
     * @return int value of size.
     */
    int getMessageValueSize() {
        m_m_value_size = sizeof(DTCMessageValue);
        return m_m_value_size;
    }
    /**
     * Read file and form a graph.
     */
    void loadGraph() {
        unsigned long long last_vertex;
        unsigned long long from;
        unsigned long long to;
        double weight = 0;
        DTCVertexValue value;
        int outdegree = 0;

        const char *line= getEdgeLine();
        sscanf(line, "%lld %lld", &from, &to);
        addEdge(from, to, &weight);
        last_vertex = from;
        ++outdegree;
        for(int64_t i = 1; i < m_total_edge; ++i){
            line = getEdgeLine();
            sscanf(line, "%lld %lld", &from, &to);
            if(last_vertex != from){
                addVertex(last_vertex, &value, outdegree);
                last_vertex = from;
                outdegree = 1;
            }
            else
                ++outdegree;
            addEdge(from, to, &weight);
        }
        addVertex(last_vertex, &value, outdegree);
    }
};

/**
 * OutputFormatter class. Write the result to file.
 */
class VERTEX_CLASS_NAME(OutputFormatter): public OutputFormatter {
public:
    /**
     * Write the result to file.
     */
    void writeResult() {
        int64_t vid;
        DTCVertexValue value;
        char s[1024];

        ResultIterator r_iter;
        r_iter.getIdValue(vid, &value);

        char token[][10] = {"in", "out", "through", "cycle"};
        for(int i = 0; i < 4; i++){
            int n = sprintf(s, "%s: %d\n", token[i], value.res[i]);
            writeNextResLine(s, n);
        }
    }
};

/**
 * An Aggregator that records a int value to compute sum.
 */
class VERTEX_CLASS_NAME(Aggregator): public Aggregator<int> {
public:
    /**
     * Initialize the local value and global value.
     */
    void init() {
        m_global = 0;
        m_local = 0;
    }
    /**
     * Read the global value.
     */
    void* getGlobal() {
        return &m_global;
    }
    /**
     * Change the global value.
     */
    void setGlobal(const void* p) {
        m_global = * (int *)p;
    }
    /**
     * Read the local value.
     */
    void* getLocal() {
        return &m_local;
    }
    /**
     * Merge the value into global.
     */
    void merge(const void* p) {
        m_global += * (int *)p;
    }
    /**
     * Accumulate the values of each vertex to local value.
     */
    void accumulate(const void* p) {
        m_local += * (int *)p;
    }
};

/**
 * Core class.
 */
class VERTEX_CLASS_NAME(): public Vertex <DTCVertexValue, double, DTCMessageValue> {
public:
    /**
     * Binary search function for accelerate the speed.
     * @param ary A list of number.
     * @param len The length of the ary.
     * @param val The value need to check if in the ary.
     * @return Boolean value for if val in the ary.
     */
    bool binarySearch(DTCMessageValue* ary, int len, DTCMessageValue val){
        int l = 0, r = len-1;
        while(l < r){
            int m = (l+r)>>1;
            if(ary[m] < val) l = m+1;
            else if(ary[m] > val) r = m-1;
            else return true;
        }
        if(len != 0 && ary[l] == val) return true;
        return false;
    }

    /**
     * Read the message list. Return an array of messages.
     * @param pmsgs the iterator pointer of message link list.
     * @return two value, the first is the pointer of the message array. the second is the length of array.
     */
    pair<DTCMessageValue*, int> getMsgList(MessageIterator* pmsgs){
        int sz = getOutEdgeIterator().size();
        DTCMessageValue* ret = (DTCMessageValue*)malloc(sizeof(DTCMessageValue) * sz);
        int cnt = 0;
        for ( ; !pmsgs->done(); pmsgs->next()){
            if(cnt >= sz){
                sz += getOutEdgeIterator().size();
                ret = (DTCMessageValue*)realloc(ret, sizeof(DTCMessageValue) * sz);
            }
            ret[cnt++] = pmsgs->getValue();
        }
        ret = (DTCMessageValue*)realloc(ret, sizeof(DTCMessageValue) * cnt);
        return make_pair(ret, cnt);
    }

    /**
     * Directed Triangle Counting Operation.
     * @param outer Outer for-loop, int* is the list, int is the length of list.
     * @param inner Inner for-loop, int* is the list, int is the length of list.
     * @param msg DTCMessageValue* is the message list, int is the length of list.
     * @param one A value reference for counting.
     * @param two A value reference for counting.
     * @param flag A flag of operation type.
     */
    void operation(pair<int*, int> outer, pair<int*, int> inner, pair<DTCMessageValue*, int> msg, int& one, int& two, bool flag){
        int* outerlist = outer.first, outerlen = outer.second;
        int* innerlist = inner.first, innerlen = inner.second;
        DTCMessageValue* msglist = msg.first;
        int msglistlen = msg.second;
        for(int i = 0; i < outerlen; i++){
            for(int j = flag ? 0 : i+1; j < innerlen; j++){
                if(flag && outerlist[i] == innerlist[j]) continue;
                if(binarySearch(msglist, msglistlen, (DTCMessageValue){outerlist[i], innerlist[j]}))
                    one++;
                if(binarySearch(msglist, msglistlen, (DTCMessageValue){innerlist[j], outerlist[i]}))
                    two++;
            }
        }
    }

    /**
     * Kernel function that executing every super step.
     * @param pmsgs the iterator pointer of message link list.
     */
    void compute(MessageIterator* pmsgs) {
        // Send the out neighbors messages.
        if(getSuperstep() == 0){
            for(OutEdgeIterator it = getOutEdgeIterator(); !it.done(); it.next())
                sendMessageTo(it.target(), (DTCMessageValue){(int)getVertexId(), (int)it.target()});
        }

        // Collect messages from super step 0 and get all in neighbors.
        // Send messages to in neighbors and out neighbors.
        else if(getSuperstep() == 1){
            pair<DTCMessageValue*, int> ret = getMsgList(pmsgs);
            DTCMessageValue* msglist = ret.first;
            int msglistlen = ret.second;
            for(int i = 0; i < msglistlen; i++)
                for(OutEdgeIterator it = getOutEdgeIterator(); !it.done(); it.next())
                    sendMessageTo(msglist[i].from, (DTCMessageValue){(int)getVertexId(), (int)it.target()});
            for(OutEdgeIterator it = getOutEdgeIterator(); !it.done(); it.next())
                sendMessageToAllNeighbors((DTCMessageValue){(int)getVertexId(), (int)it.target()});
            free(msglist);
        }

        // Counting Directed Triangles.
        else if(getSuperstep() == 2){
            // Get a sorted messages list.
            pair<DTCMessageValue*, int> ret = getMsgList(pmsgs);
            DTCMessageValue* msglist = ret.first;
            int msglistlen = ret.second;
            sort(msglist, msglist + msglistlen);

            // Get a in neighbors list.
            int inlen = 0, previn = -1;
            int* inlist = (int*)malloc(sizeof(int) * msglistlen);
            for(int i = 0; i < msglistlen; i++){
                if(msglist[i].to == getVertexId() && previn != msglist[i].from){
                    inlist[inlen++] = msglist[i].from;
                    previn = msglist[i].from;
                }
            }
            inlist = (int*)realloc(inlist, sizeof(int) * inlen);

            // Get a out neighbors list.
            int outlen = 0;
            int* outlist = (int*)malloc(sizeof(int) * getOutEdgeIterator().size());
            for(OutEdgeIterator it = getOutEdgeIterator(); !it.done(); it.next())
                outlist[outlen++] = (int)it.target();

            // Calculate the four statistical value of current vertex.
            int acc[4] = {};
            operation(make_pair(inlist, inlen), make_pair(inlist, inlen), ret, acc[0], acc[0], false);
            operation(make_pair(outlist, outlen), make_pair(outlist, outlen), ret, acc[1], acc[1], false);
            operation(make_pair(outlist, outlen), make_pair(inlist, inlen), ret, acc[3], acc[2], true);

            // Free the malloc memory.
            free(inlist);
            free(outlist);
            free(msglist);

            // Add to aggregator value.
            for(int i = 0; i < 4; i++)
                accumulateAggr(i, &acc[i]);

        }

        // Ready to stop.
        else{
            for(int i = 0; i < 4; i++)
                if(*(int*)getAggrGlobal(i) != getValue().res[i])
                    mutableValue()->res[i] = *(int*)getAggrGlobal(i);
            voteToHalt();
        }

    }
};

/**
 * Graph class.
 */
class VERTEX_CLASS_NAME(Graph): public Graph {
public:
    /** Statistical value need to be stored globally. */
    VERTEX_CLASS_NAME(Aggregator)* aggregator;

public:
    /**
     * Initialize for a graph. Register aggregators and so on. Get arguments from command line.
     */
    void init(int argc, char* argv[]) {
        setNumHosts(5);
        setHost(0, "localhost", 1411);
        setHost(1, "localhost", 1421);
        setHost(2, "localhost", 1431);
        setHost(3, "localhost", 1441);
        setHost(4, "localhost", 1451);

        if (argc < 3) {
           printf ("Usage: %s <input path> <output path>\n", argv[0]);
           exit(1);
        }

        m_pin_path = argv[1];
        m_pout_path = argv[2];

        aggregator = new VERTEX_CLASS_NAME(Aggregator)[4];
        regNumAggr(4);
        regAggr(0, &aggregator[0]);
        regAggr(1, &aggregator[1]);
        regAggr(2, &aggregator[2]);
        regAggr(3, &aggregator[3]);
    }

    /**
     * Terminate function when destroy a graph. Free aggregators memory.
     */
    void term() {
        delete[] aggregator;
    }
};

/* STOP: do not change the code below. */
extern "C" Graph* create_graph() {
    Graph* pgraph = new VERTEX_CLASS_NAME(Graph);

    pgraph->m_pin_formatter = new VERTEX_CLASS_NAME(InputFormatter);
    pgraph->m_pout_formatter = new VERTEX_CLASS_NAME(OutputFormatter);
    pgraph->m_pver_base = new VERTEX_CLASS_NAME();

    return pgraph;
}

extern "C" void destroy_graph(Graph* pobject) {
    delete ( VERTEX_CLASS_NAME()* )(pobject->m_pver_base);
    delete ( VERTEX_CLASS_NAME(OutputFormatter)* )(pobject->m_pout_formatter);
    delete ( VERTEX_CLASS_NAME(InputFormatter)* )(pobject->m_pin_formatter);
    delete ( VERTEX_CLASS_NAME(Graph)* )pobject;
}
