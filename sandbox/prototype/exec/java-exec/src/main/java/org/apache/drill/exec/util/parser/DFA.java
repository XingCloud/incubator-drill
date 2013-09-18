package org.apache.drill.exec.util.parser;

import com.xingcloud.meta.HBaseFieldInfo;
import com.xingcloud.meta.KeyPart;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: yangbo
 * Date: 8/5/13
 * Time: 10:47 AM
 * To change this template use File | Settings | File Templates.
 */
public class DFA {

  private static boolean[] wordTable;
  private static boolean[] textTable;
  private static boolean[] binaryTable;
  private static boolean[][] type2Table;
  static{
    wordTable = new boolean[256];
    Arrays.fill(wordTable, false);
    /*
    Arrays.fill(wordTable, 48, 58, true);
    Arrays.fill(wordTable, 65, 91, true);
    Arrays.fill(wordTable, 97, 123, true);*/

    Arrays.fill(wordTable,33,46,true);
    Arrays.fill(wordTable,47,127,true);

    textTable = new boolean[256];
    Arrays.fill(textTable, false);
    Arrays.fill(textTable, 32, 127, true); 
    binaryTable = new boolean[256];
    Arrays.fill(binaryTable, true);
    
    type2Table = new boolean[3][];
    type2Table[byteType.Word.ordinal()]=wordTable;
    type2Table[byteType.Text.ordinal()]=textTable;
    type2Table[byteType.Binary.ordinal()]=binaryTable;   
    
  }
 
    private State start;
    private State end;
    private List<KeyPart> keyParts;
    private Map<String,HBaseFieldInfo> infoMap;
    private Map<KeyPart,State> kpStateMap;
  
    public DFA(List<KeyPart> keyPartList, Map<String, HBaseFieldInfo> fieldInfoMap){
        this.keyParts=keyPartList;   //To change body of created methods use File | Settings | File Templates.
        this.infoMap=fieldInfoMap;
        this.start=new State(null);
        this.start.name="start";
        this.end=new State(null);
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
  
  private void init() {
    State state = start;
    buildNexts(state, keyParts, 0, Arrays.asList(end));
    buildStates(keyParts, Arrays.asList(end));
  }

  private void buildStates(List<KeyPart> list, List<State> possibleEnds) {
    for (int i = 0; i < list.size(); i++) {
      KeyPart curKp = list.get(i);
      switch (curKp.getType()) {
        case field:
        case constant:
          State state = getCreateStateFor(curKp);
          buildNexts(state, list, i + 1, possibleEnds);
          break;
        case optionalgroup:
          List<State> possibleNexts = new ArrayList<>();
          gatherNextsFromList(list, i+1, possibleNexts);
          buildStates(curKp.getOptionalGroup(), possibleNexts);
      }
    }

  }

  private void buildNexts(State state, List<KeyPart> keyParts, int nextIndex, List<State> possibleEnds) {
    List<State> possibleNexts = new ArrayList<>();
    boolean includeEnd = gatherNextsFromList(keyParts, nextIndex, possibleNexts);
    for (int i = 0; i < possibleNexts.size(); i++) {
      State nextState = possibleNexts.get(i);
      markTransition(state, nextState);
    }
    if(includeEnd){
      for (int i = 0; i < possibleEnds.size(); i++) {
        State end = possibleEnds.get(i);
        if(end == DFA.this.end){
          for (int j = 0; j < state.nexts.length; j++) {
            if (state.nexts[j] == null) {
              state.nexts[j] = end;
            }
          }
        }else{
          markTransition(state, end);
        }
      }
    }
  }

  private void markTransition(State state, State nextState) {
    switch(nextState.kp.getType()){
      case constant:
        byte c = nextState.kp.getSerializedConstant()[0];
        state.nexts[toUInt(c)] = nextState;
        break;
      case field:
        boolean[] copyFrom = type2Table[nextState.type.ordinal()];
        for (int j = 0; j < copyFrom.length; j++) {
          boolean b = copyFrom[j];
          if(state.nexts[j]!=null){
            throw new IllegalArgumentException("state collapse! in state:"+state+";["+j+"], old:"+state.nexts[j]+",new:"+nextState);
          }
          if(b){
            state.nexts[j] = nextState;
          }
        }
        break;
      default:
        throw new IllegalStateException("can not next to:"+nextState);
    }
  }

  private State getCreateStateFor(KeyPart keyPart) {
    State state = kpStateMap.get(keyPart);
    if(state == null){
      state = new State(keyPart);
      kpStateMap.put(keyPart, state);
    }
    return state;
  }

  /**
   * 
   * @param list
   * @param nextIndex
   * @param possibleNexts
   * @return 如果收集到list的结尾，有可能需要包括end状态，则返回true，否则返回false
   */
  private boolean gatherNextsFromList(List<KeyPart> list, int nextIndex, List<State> possibleNexts){
    int i = nextIndex;
    loop:
    for(;i< list.size();i++){
      KeyPart part = list.get(i);
      switch (part.getType()){
        case field:
        case constant:
          possibleNexts.add(getCreateStateFor(part));
          break loop;//确定到这里，结束搜索
        case optionalgroup:
          gatherOptionalNexts(part, possibleNexts);
          break;//可能可以选下一个, 继续搜索
        default:
          throw new IllegalStateException("cannot process:"+part.getType());
      }
    }
    return i == list.size();
  }
  private void gatherOptionalNexts(KeyPart part, List<State> possibleNexts) {
    List<KeyPart> optionalGroup = part.getOptionalGroup();
    gatherNextsFromList(optionalGroup, 0, possibleNexts);
  }

    private byteType getType(KeyPart kp){
        if(kp==null)return byteType.Binary;
        if(kp.getType() == KeyPart.Type.constant)
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

  public DFAMatcher newMatcher(byte[] target){
    return new DFAMatcher(target);
  }

    public class State{
       public String name;
       public int size;
       public byteType type;
       public State[] nexts = new State[256];
       public KeyPart kp;

      public State(KeyPart kp) {
        //keypart
        this.kp = kp;
        //name
        if (kp != null) {
          if (kp.getType() == KeyPart.Type.field)
            this.name = kp.getField().getName();
          else if (kp.getType() == KeyPart.Type.constant)
            this.name = kp.getConstant();
        //size
        switch (kp.getType()) {
          case field:
            HBaseFieldInfo info = infoMap.get(kp.getField().getName());
            size = info.serLength;
            break;
          case constant:
            size = kp.getSerializedConstant().length;
            break;
          default:
            throw new IllegalArgumentException("do not support State for:" + kp.getType());
        }
        //type
        type=getType(kp);        
        
        //build nexts for var length text/word/binary
        if(kp.getType() == KeyPart.Type.field && size == 0){
          boolean[] copyFrom = null;
          switch (type){
            case Binary:
              copyFrom = binaryTable;
              break;
            case Text:
              copyFrom = textTable;
              break;
            case Word:
              copyFrom = wordTable;
              break;
            default:
              throw new IllegalArgumentException("do not support State for:"+type);
          }
          for (int i = 0; i < copyFrom.length; i++) {
            if(copyFrom[i]){
              nexts[i] = this;
            }
          }
        }
        }
          
      }


      @Override
      public String toString() {
        return "State["+name+"]";
      }
    }

    public enum byteType{
        Word,
        Text,
        Binary,

    }

  public class DFAMatcher{

    State current;
    int nextPosition;
    byte[] target;

    public DFAMatcher(byte[] target) {
      resetTo(target, begin(), 0);
    }

    
    public void resetTo(byte[] target) {
      resetTo(target, start, 0);
    }
    
    public void resetTo(byte[] target, State state, int nextPosition){
      this.target = target;
      this.current = state;
      this.nextPosition = nextPosition;
    }
    
    public void resetTo(byte[] target, KeyPart keyPart, int nextPosition){
      resetTo(target, kpStateMap.get(keyPart), nextPosition);
    }

    /**
     * 
     * @return 返回下一个Field对应的位置。不返回constant对应的位置
     */
    public FieldPosition nextField(){
      FieldPosition ret = nextPart();
      while(ret!=null && ret.fieldDef.getType()!= KeyPart.Type.field){
        ret = nextPart();
      }
      return ret;
    }

    FieldPosition nextPart(){
      if(current == end){
        return null;
      }
      byte c = target[nextPosition];
      if(current == start){
        if(current.nexts[toUInt(c)] == null){
          throw new NullPointerException("current state:"+current+", next char:"+c+", no next status!");
        }          
        current = start.nexts[toUInt(c)];
        if(current == end){
          return null;
        }
      }
      
      if(current.size>0){
        int start = nextPosition;
        int end = nextPosition + current.size;
        nextPosition += current.size;
        FieldPosition ret = new FieldPosition(current.kp, start, end);
        if(nextPosition>=target.length){
          current = DFA.this.end;
        }else{
          c = target[end];
          if(current.nexts[toUInt(c)] == null){
            throw new NullPointerException("current state:"+current+", next char:"+c+", no next status!");
          }
          current = current.nexts[toUInt(c)];
        }
        return ret;
      }else{
        int start = nextPosition;
        while(current == current.nexts[toUInt(c)]){
          nextPosition++;
          if(nextPosition >= target.length){
            break;
          }
          c = target[nextPosition];
        }
        int end = nextPosition;
        FieldPosition ret = new FieldPosition(current.kp, start, end);
        if(nextPosition>=target.length){
          current = DFA.this.end;
        }else{
          if(current.nexts[toUInt(c)] == null){
            throw new NullPointerException("current state:"+current+", next char:"+c+", no next status!");
          }          
          current = current.nexts[toUInt(c)];
        }
        return ret;

      }
    }

  }
  
  static int toUInt(byte c){
    return 0xff & c;
  }
  
  public static class FieldPosition{
    KeyPart fieldDef;
    int start;
    int end;

    public FieldPosition(KeyPart fieldDef, int start, int end) {
      this.fieldDef = fieldDef;
      this.start = start;
      this.end = end;
    }
  }
}
