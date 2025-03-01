#ifndef VECTOR_HPP
#define VECTOR_HPP

#include <stdexcept> // Για διαχείριση εξαιρέσεων

template <typename T>
class Vector {
private:
    T* data;         // Δείκτης στον δυναμικό πίνακα
    size_t capacity; // Τρέχουσα χωρητικότητα του vector
    size_t size;     // Πλήθος των στοιχείων στο vector

    // Συνάρτηση για να αλλάζει το μέγεθος του πίνακα όταν γεμίσει
    void resize(size_t new_capacity);

public:
    Vector();
    Vector(const Vector<T>& other);     // Copy constructor
    ~Vector();

    void push_back(const T& value);
    void pop_back();

    T& operator[](size_t index);
    const T& operator[](size_t index) const;
    Vector<T>& operator=(const Vector<T>& other);
    size_t get_size() const;
    size_t get_capacity() const;

    bool find(const T& value) const; 
};

// Υλοποιήσεις συναρτήσεων template

template <typename T>
Vector<T>::Vector() : data(nullptr), capacity(0), size(0) {}

template <typename T>
Vector<T>::Vector(const Vector<T>& other) : data(nullptr), capacity(other.capacity), size(other.size) {
    // Αντιγραφή του πίνακα δεδομένων
    if (capacity > 0) {
        data = new T[capacity];
        for (size_t i = 0; i < size; ++i) {
            data[i] = other.data[i];
        }
    }
}

template <typename T>
Vector<T>::~Vector() {
    delete[] data;
}

template <typename T>
void Vector<T>::resize(size_t new_capacity) {
    T* new_data = new T[new_capacity]; // Δέσμευση νέας μνήμης
    for (size_t i = 0; i < size; i++) {
        new_data[i] = data[i]; // Αντιγραφή των παλιών στοιχείων στον νέο πίνακα
    }
    delete[] data;  // Διαγραφή του παλιού πίνακα
    data = new_data; // Δείκτης στον νέο πίνακα
    capacity = new_capacity;
}

template <typename T>
void Vector<T>::push_back(const T& value) {
    if (size == capacity) {
        resize(capacity == 0 ? 1 : 2 * capacity); // Διπλασιασμός της χωρητικότητας
    }
    data[size++] = value; // Προσθήκη στοιχείου και αύξηση του μεγέθους
}

template <typename T>
void Vector<T>::pop_back() {
    if (size > 0) {
        size--; // Μείωση του μεγέθους (δεν χρειάζεται αποδέσμευση μνήμης)
    } else {
        throw std::out_of_range("Το vector είναι άδειο");
    }
}

template <typename T>
T& Vector<T>::operator[](size_t index) {
    if (index >= size) {
        throw std::out_of_range("Ο δείκτης είναι εκτός ορίων");
    }
    return data[index];
}

template <typename T>
const T& Vector<T>::operator[](size_t index) const {
    return data[index];
}

template <typename T>
Vector<T>& Vector<T>::operator=(const Vector<T>& other) {
    if (this != &other) { // Έλεγχος για αυτοαντιστοίχιση (self-assignment)
        delete[] data; // Αποδέσμευση τρέχοντος πίνακα

        // Αντιγραφή των δεδομένων
        capacity = other.capacity;
        size = other.size;
        data = new T[capacity];
        for (size_t i = 0; i < size; ++i) {
            data[i] = other.data[i];
        }
    }
    return *this; // Επιστροφή της τρέχουσας κλάσης
}

template <typename T>
size_t Vector<T>::get_size() const {
    return size;
}

template <typename T>
size_t Vector<T>::get_capacity() const {
    return capacity;
}

template <typename T>
bool Vector<T>::find(const T& value) const {
    for (size_t i = 0; i < size; ++i) {
        if (data[i] == value) {
            return true; 
        }
    }
    return false; 
}

#endif // VECTOR_HPP
