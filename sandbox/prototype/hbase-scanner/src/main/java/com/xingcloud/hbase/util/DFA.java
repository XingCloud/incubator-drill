package com.xingcloud.hbase.util;

import com.xingcloud.meta.HBaseFieldInfo;
import com.xingcloud.meta.KeyPart;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: yangbo
 * Date: 8/5/13
 * Time: 10:47 AM
 * To change this template use File | Settings | File Templates.
 */
public class DFA {
    private State start;
    private State end;
    private List<KeyPart> keyParts;
    private Map<String,HBaseFieldInfo> infoMap;
    private Map<KeyPart,State> kpStateMap;
    private byteType[] inputTypes;
    public  DFA(List<KeyPart> keyPartList,Map<String,HBaseFieldInfo> fieldInfoMap){
        this.keyParts=keyPartList;   //To change body of created methods use File | Settings | File Templates.
        this.infoMap=fieldInfoMap;
        this.start=new State(null,null,false);
        this.start.name="start";
        this.end=new State(start,null,true);
        this.end.name="end";
        kpStateMap=new HashMap<>();
        init();
    }
    public State begin(){
        return start;
    }
    public State end(){
        return end;
    }

    public State next(State curState, byte input){
        curState.len++;
        if(curState.len > curState.size) {
            byteType type = getType(input);
            return curState.nexts.get(type);
        }
        else return curState;
    }

    private void init(){
        State state=start;
        for(int i=0;i<keyParts.size()-1;i++){
             KeyPart curKp=keyParts.get(i);
             KeyPart nextKp=keyParts.get(i+1);
             state=insert(state,curKp,nextKp,false);
        }
        state=insert(state,keyParts.get(keyParts.size()-1),null,false);
        setNext(state,end,null);
        state.directNext=end;
        state.size=
          infoMap.get(keyParts.get(keyParts.size()-1).getField().getName()).serLength;
        inputTypes=new byteType[256];
        for(int input=0;input<256;input++){
            if((input>=48&&input<=57)||(input>=65&&input<=90)||(input>=97&&input<=122)){
                inputTypes[input]=byteType.Word;
            }
            else if(input>=32&&input<=126)
                inputTypes[input]=byteType.Text;
            else
                inputTypes[input]=byteType.Binary;
        }

    }

    private State insert(State state, KeyPart curKp,KeyPart nextKp,boolean isEnd) {
        if(curKp.getType()!= KeyPart.Type.optionalgroup){
            if(!kpStateMap.containsKey(curKp)){
                State nextState=new State(state,curKp,isEnd);
                if(curKp.getType()== KeyPart.Type.field)
                {
                    HBaseFieldInfo info=infoMap.get(curKp.getField().getName());
                    nextState.size=info.serLength;
                }
                else{
                    nextState.size=curKp.getSerializedConstant().length;
                }
                nextState.type=getType(curKp);
                kpStateMap.put(curKp,nextState);
            }
            State nextState=kpStateMap.get(curKp);
            setNext(state,nextState,curKp);
            state.directNext=nextState;
            return nextState;
        }
        else{
            List<KeyPart> opKps=curKp.getOptionalGroup();

            KeyPart firstKp=opKps.get(0);
            State opState=new State(state,firstKp,isEnd);
            if(firstKp.getType()== KeyPart.Type.field)
            {
                HBaseFieldInfo info=infoMap.get(firstKp.getField().getName());
                opState.size=info.serLength;
            }
            else{
                opState.size=curKp.getSerializedConstant().length;
            }
            opState.type=getType(firstKp);
            if(!kpStateMap.containsKey(firstKp))
                kpStateMap.put(firstKp,opState);

            byteType opBt=opState.type;
            state.nexts.put(opBt,opState);

            State nextState;
            if(nextKp==null)
                 nextState=end;
            else
            {
                if(!kpStateMap.containsKey(nextKp)){
                    nextState =new State(state,nextKp,isEnd);
                    if(nextKp.getType()== KeyPart.Type.constant)
                        nextState.size=nextKp.getSerializedConstant().length;
                    else {
                        HBaseFieldInfo info=infoMap.get(nextKp.getField().getName());
                        nextState.size=info.serLength;
                    }
                    kpStateMap.put(nextKp,nextState);
                }
                nextState=kpStateMap.get(nextKp);
            }
            for(byteType bt: getSideType(opBt)){
                state.nexts.put(bt,nextState);
            }


            for(int i=1;i<opKps.size()-1;i++){
                KeyPart opCurKp=opKps.get(i);
                KeyPart opNextKp=opKps.get(i+1);
                opState=insert(opState,opCurKp,opNextKp,false);
            }
            if(opKps.size()!=1)
                opState=insert(opState,opKps.get(opKps.size()-1),nextKp,false);
            return opState;
        }

    }

    private void setNext(State state,State nextState,KeyPart kp){
        byteType bt=getType(kp);
        setTypeNext(state,nextState,bt);
        if(state.size==0&&state!=start){
            byteType curBt=getType(state.kp);
            setTypeNext(state,state,curBt);
        }

    }

    private void setTypeNext(State state,State nextState,byteType bt){
        switch (bt){
            case Binary:
                state.nexts.put(byteType.Binary,nextState);
                state.nexts.put(byteType.Text,nextState);
                state.nexts.put(byteType.Word,nextState);
                break;
            case Text:
                state.nexts.put(byteType.Word,nextState);
                state.nexts.put(byteType.Text,nextState);
                break;
            case Word:
                state.nexts.put(byteType.Word,nextState);
        }

    }


    private byteType getType(KeyPart kp){
        if(kp==null)return byteType.Binary;
        if(kp.getType()== KeyPart.Type.constant)
            return byteType.Binary;
        else {
            switch (infoMap.get(kp.getField().getName()).serType){
                case  WORD:
                    return byteType.Word;
                case  TEXT:
                    return byteType.Text;
                case  BINARY:
                    return byteType.Binary;
                default:
                    return byteType.Binary;
            }
        }
    }

    private byteType getType(byte input) {
        if((input>=48&&input<=57)||(input>=65&&input<=90)||(input>=97&&input<=122)){
            return byteType.Word;
        }
        else if(input>=32&&input<=126)
            return byteType.Text;
        else
            return byteType.Binary;
    }

    private List<byteType> getSideType(byteType type){
        List<byteType> typeList=new ArrayList<>();
        switch (type){
            case Word:
                typeList.add(byteType.Text);
                typeList.add(byteType.Binary);
                break;
            case Text:
                typeList.add(byteType.Binary);
                break;
        }
        return typeList;
    }

    public void reset() {
        for(Map.Entry<KeyPart,State> entry: kpStateMap.entrySet()){
          entry.getValue().len=0;
        }
    }


    public class State{
       public String name;
       public int size;
       public int len;
       public byteType type;
       public Map<byteType,State> nexts;
       public State directNext;
       public State prev;
       public boolean isEnd;
       public KeyPart kp;

       public State(State prev,KeyPart kp,boolean isEnd){
           this.isEnd=isEnd;
           this.prev=prev;
           this.kp=kp;
           if(kp!=null){
               if(kp.getType()== KeyPart.Type.field)
                   this.name=kp.getField().getName();
               else if(kp.getType()== KeyPart.Type.constant)
                   this.name=kp.getConstant();
           }
           this.size=this.len=0;
           this.isEnd=isEnd;
           nexts=new HashMap<>();
       }

    }

    public enum byteType{
        Word,
        Text,
        Binary,

    }

    public enum TypeName {
        Text,
        Word,
        Binary
    }


}
