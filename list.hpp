#ifndef LIST_HPP
#define LIST_HPP

#include <iostream>

template <typename T>
class List {
public:
    struct ListNode {
        T data;       
        ListNode* next; 
        ListNode(T value) : data(value), next(nullptr) {}
    };

private:
    ListNode* head;  
    ListNode* tail;  
    int size; 
    void copyFrom(const List<T>& other);

public:
    List() : head(nullptr), tail(nullptr), size(0) {}
    List(const List<T>& other);
    ~List();
    
    List<T>& operator=(const List<T>& other);

    void add(T value);
    void remove(T value);
    void print() const;

    int getSize() const { return size; } 
    ListNode* getHead() const {return head;}
    ListNode* getNext(ListNode* node) const {return node->next;}
    T getData(ListNode* node) const {return node->data;}
};

template <typename T>
List<T>::List(const List<T>& other) : head(nullptr), tail(nullptr), size(0) {
    copyFrom(other);
}

template <typename T>
List<T>::~List() {
    ListNode* current = head;
    while (current != nullptr) {
        ListNode* next = current->next;
        delete current;
        current = next;
    }
}

template <typename T>
void List<T>::add(T value) {
    ListNode* newNode = new ListNode(value);
    if (tail == nullptr) {  // Αν η λίστα είναι κενή
        head = tail = newNode;
    } else {
        tail->next = newNode;  // Προσθήκη στο τέλος
        tail = newNode;
    }
    size++;
}

template <typename T>
void List<T>::remove(T value) {
    ListNode* current = head;
    ListNode* previous = nullptr;

    while (current != nullptr) {
        if (current->data == value) {
            if (previous == nullptr) {  // Αφαίρεση του πρώτου κόμβου
                head = current->next;
            } else {
                previous->next = current->next;
            }

            if (current == tail) {  // Αν είναι το τελευταίο στοιχείο
                tail = previous;
            }

            delete current;  // Αποδέσμευση μνήμης
            size--;
            return;
        }

        previous = current;
        current = current->next;
    }
}

template <typename T>
void List<T>::print() const {
    ListNode* current = head;
    while (current != nullptr) {
        std::cout << current->data << " ";
        current = current->next;
    }
    std::cout << std::endl;
}

template <typename T>
List<T>& List<T>::operator=(const List<T>& other) {
    if (this != &other) {  // Έλεγχος για αυτοαντιστοίχιση
        // Αποδέσμευση των υπαρχόντων κόμβων
        ListNode* current = head;
        while (current != nullptr) {
            ListNode* next = current->next;
            delete current;
            current = next;
        }
        head = nullptr;
        tail = nullptr;
        size = 0;

        // Αντιγραφή των κόμβων από την άλλη λίστα
        copyFrom(other);
    }
    return *this;
}

template <typename T>
void List<T>::copyFrom(const List<T>& other) {
    ListNode* current = other.head;
    while (current != nullptr) {
        add(current->data);  // Προσθήκη του κάθε κόμβου στη νέα λίστα
        current = current->next;
    }
}

#endif // LIST_HPP
