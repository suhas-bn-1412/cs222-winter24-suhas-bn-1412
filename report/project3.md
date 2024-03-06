## Project 3 Report


### 1. Basic information
- Team #: 4
- Github Repo Link: https://github.com/suhas-bn-1412/cs222-winter24-suhas-bn-1412
- Student 1 UCI NetID: `snhegde`
- Student 1 Name: Sujay Narashimamurthy Hegde
- Student 2 UCI NetID (if applicable): `sbasappa`
- Student 2 Name (if applicable): Suhas Basappa Nataraj


### 2. Meta-data page in an index file
- Show your meta-data page of an index design if you have any.
  - We employ a metadata page containing a single unsigned integer value,
    which denotes the page number of the root page



### 3. Index Entry Format
- Show your index entry design (structure). 

  - entries on internal nodes:  
    ```
        class PageNumAndKey {
        private:
          unsigned int _pageNum = 0;
          int _intKey = 0;
          float _floatKey = float(0);
          std::string _stringKey = "";
          bool valid = false;
        public:
          // getters, setters
        }
    ```
  
  - entries on leaf nodes:
    ```
    class RidAndKey {
    private:
        RID _rid;
        int _intKey = 0;
        float _floatKey = float(0);
        std::string _stringKey = "";
    public:
    // getters, setters
    }
    ```


### 4. Page Format
- Show your internal-page (non-leaf node) design.
  ```
  class NonLeafPage {
    std::vector<PageNumAndKey> pageNumAndKeys; // last element shall contain only a valid pageNum
    AttrType keyType;
    unsigned int freeByteCount;
  }
  ```


- Show your leaf-page (leaf node) design.
```
  class LeafPage {
    std::vector<KeyAndRid> keyAndRidPairs;
    AttrType keyType;
    unsigned int freeByteCount;
    int nextPageNum; // shall be -1 if this is the last leaf page.
  }
  ```




### 5. Describe the following operation logic.
- Split



- Rotation (if applicable)



- Merge/non-lazy deletion (if applicable)



- Duplicate key span in a page
  - We associate and store a single RID for every key.
  Duplicate keys, if any, are stored consecutively.


- Duplicate key span multiple pages (if applicable)



### 6. Implementation Detail
- Have you added your own module or source file (.cc or .h)? 
  Clearly list the changes on files and CMakeLists.txt, if any.
  - Modules added:
    - None
  - Source files/ headers added:
      - nonLeafPage.c / .h
      - leafPage.c / .h
      - VarcharSerdes.c / .h
      - PageSerializer.c / .h
      - PageDeserializer.c / .h

- Other implementation details:



### 7. Member contribution (for team of two)
- Explain how you distribute the workload in team.``
  1. Sujay Hegde:
     - Page (leaf/non-leaf) formats, interfaces definition 
     - Page Serializer, Deserializer
     - `IX.delete()`
     - `IX.scan()`

  2. Suhas Basappa Nataraj:
    - `IX.insert()`
    - `IX.printBTree()`

### 8. Other (optional)
- Freely use this section to tell us about things that are related to the project 3, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)
