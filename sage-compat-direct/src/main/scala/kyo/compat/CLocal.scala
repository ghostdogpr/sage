package kyo.compat

/**
  * Uses an `InheritableThreadLocal[A]`. `let` installs a value for the duration of a computation on the current thread and restores the
  * previous value afterward. Virtual threads started by this binding inherit the value that is current when they start.
  */
opaque type CLocal[A] = InheritableThreadLocal[A]

object CLocal {

  /**
    * Allocates a fresh local whose value is `default` on every thread until `let` changes it.
    */
  def init[A](default: => A): CIO[CLocal[A]] =
    CIO.defer {
      val initial = default
      new InheritableThreadLocal[A] {
        override def initialValue(): A = initial
      }
    }

  /**
    * Wraps a native `InheritableThreadLocal` as a `CLocal`. The conversion is the identity on the carrier.
    */
  inline def lift[A](inline u: InheritableThreadLocal[A]): CLocal[A] = u

  extension [A](self: CLocal[A]) {

    /**
      * Unwraps to the native `InheritableThreadLocal`. The conversion is the identity on the carrier.
      */
    inline def lower: InheritableThreadLocal[A] = self

    /**
      * Reads the current thread's value.
      */
    inline def get: CIO[A] = CIO.defer(self.get())

    /**
      * Installs `v` while `c` runs, then restores the previous value.
      */
    def let[B](v: => A)(c: => CIO[B]): CIO[B] = self.update(_ => v)(c)

    /**
      * Installs `f` applied to the current value while `c` runs, then restores the previous value.
      */
    def update[B](f: A => A)(c: => CIO[B]): CIO[B] =
      CIO.around(c) { run =>
        val previous = self.get()
        self.set(f(previous))
        try run()
        finally self.set(previous)
      }

  }

}
